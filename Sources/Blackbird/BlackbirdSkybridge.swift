//
//           /\
//          |  |                       Blackbird
//          |  |
//         .|  |.       https://github.com/marcoarment/Blackbird
//         $    $
//        /$    $\          Copyright 2022–2023 Marco Arment
//       / $|  |$ \          Released under the MIT License
//      .__$|  |$__.
//           \/
//
//  BlackbirdSkybridge.swift
//
//  Permission is hereby granted, free of charge, to any person obtaining a copy
//  of this software and associated documentation files (the "Software"), to deal
//  in the Software without restriction, including without limitation the rights
//  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//  copies of the Software, and to permit persons to whom the Software is
//  furnished to do so, subject to the following conditions:
//
//  The above copyright notice and this permission notice shall be included in all
//  copies or substantial portions of the Software.
//
//  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
//  SOFTWARE.
//

#if canImport(CloudKit)

import Foundation
import CloudKit
import CryptoKit
import zlib
@preconcurrency import Combine
import os.log

/// A BlackbirdModel type that ``BlackbirdSkybridge`` can sync to CloudKit.
///
/// Conforming types must declare one bookkeeping column, which Skybridge manages:
///
///     @BlackbirdColumn var skybridgeMetadata: Data?
///
/// Syncing is opt-out per column: every column is sent to CloudKit except the
/// primary key (which is embedded in the record name), `skybridgeMetadata`, and
/// any columns listed in ``skybridgeExcludedColumns``.
///
/// Because new columns sync by default, adding a column to a conforming type is
/// a sync-schema decision: its name must be a valid CloudKit field name, its
/// values must fit CloudKit's record size limits, and it should be optional or
/// have a sensible default, since rows created from server records that predate
/// it decode it as `NULL`. Columns that must stay device-local belong in
/// ``skybridgeExcludedColumns``.
///
/// Requirements: an explicitly declared, single-column, non-Data `primaryKey`.
public protocol BlackbirdSkybridgeSyncable: BlackbirdModel {
    /// Opaque sync bookkeeping (the last-synced record archive and the
    /// last-write-wins conflict timestamp). Managed by Skybridge.
    var skybridgeMetadata: Data? { get set }

    /// Columns that should NOT be sent to CloudKit, e.g. `[ \.$localDraft ]`. Default: none.
    static var skybridgeExcludedColumns: [BlackbirdColumnKeyPath] { get }

    /// `Data?` columns whose contents sync as `CKAsset`s on the record instead
    /// of inline field values, e.g. `[ \.$image ]` — for blobs that could
    /// exceed CloudKit's inline record size limits. Default: none.
    ///
    /// Asset contents still live in the local column: uploads write them out
    /// to temporary files, and fetched assets are read back into the column,
    /// so every synced asset is always fully present locally.
    static var skybridgeAssetColumns: [BlackbirdColumnKeyPath] { get }
}

extension BlackbirdSkybridgeSyncable {
    public static var skybridgeExcludedColumns: [BlackbirdColumnKeyPath] { [] }
    public static var skybridgeAssetColumns: [BlackbirdColumnKeyPath] { [] }
}

private let skybridgeMetadataColumnName = "skybridgeMetadata"

/// The decoded contents of a row's `skybridgeMetadata` column.
///
/// Two formats exist. The original stored a full `CKRecord` archive of the record as last synced
/// (`ckRecordData`), which duplicated every synced value in each row and had to be unarchived to
/// compare against. The current format keeps only what's needed: the record's system fields (so
/// uploads carry the right change tag), a fingerprint of the synced content as last confirmed
/// with the server, and the digest of each asset column. Rows in the original format are
/// upgraded in place the first time they're touched, and by a background pass at startup, so
/// existing databases keep working unchanged.
internal struct SkybridgeMetadata: Codable, Sendable {
    /// Original format: an archive of the whole CKRecord as last synced. Read only to upgrade.
    var ckRecordData: Data? = nil

    /// Interim format: an uncompressed archive of the last-synced record's system fields. Read only.
    var systemFields: Data? = nil

    /// Current format: the system-fields archive compressed against a per-record-type template
    /// (see ``SkybridgeArchiveCodec``), typically under 100 bytes instead of ~1.7 KB.
    var packedSystemFields: Data? = nil

    /// Current format: fingerprint (SHA-256, 32 bytes) of the synced columns, asset digests, and
    /// last-write timestamp as last confirmed with the server. A row whose own fingerprint matches
    /// has no local edits.
    var syncedFingerprint: Data? = nil

    /// Current format: digest of each asset column's bytes as last synced, keyed by column name.
    var assetDigests: [String: String]? = nil

    /// Current format: the last-write timestamp as last confirmed with the server. Decides whether
    /// a local edit needs a fresh timestamp (the writer didn't bump it) before uploading.
    var syncedLastModifiedDate: Date? = nil

    /// The local-edit timestamp used for last-write-wins conflict resolution.
    var syncLastModifiedDate: Date? = nil

    /// Asset columns whose server bytes couldn't be read when the record was
    /// applied: the column is NULL locally while the server has an asset.
    /// Distinguishes "not downloaded yet" from "deleted here" — without it, a
    /// pending row would read as a local deletion and destroy the server's
    /// asset on the next upload.
    var pendingAssetColumns: [String]? = nil

    init() { }

    init(ckRecordData: Data? = nil, syncLastModifiedDate: Date? = nil) {
        self.ckRecordData = ckRecordData
        self.syncLastModifiedDate = syncLastModifiedDate
    }

    private enum CodingKeys: String, CodingKey {
        case ckRecordData, systemFields, packedSystemFields, syncedFingerprint, assetDigests, syncedLastModifiedDate, syncLastModifiedDate, pendingAssetColumns
    }

    init(from decoder: any Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        ckRecordData = try c.decodeIfPresent(Data.self, forKey: .ckRecordData)
        systemFields = try c.decodeIfPresent(Data.self, forKey: .systemFields)
        packedSystemFields = try c.decodeIfPresent(Data.self, forKey: .packedSystemFields)
        // The fingerprint was briefly stored as a hex string; accept either form.
        if let data = try? c.decodeIfPresent(Data.self, forKey: .syncedFingerprint) {
            syncedFingerprint = data
        } else if let hex = try? c.decodeIfPresent(String.self, forKey: .syncedFingerprint) {
            syncedFingerprint = Data(hex: hex)
        }
        assetDigests = try c.decodeIfPresent([String: String].self, forKey: .assetDigests)
        syncedLastModifiedDate = try c.decodeIfPresent(Date.self, forKey: .syncedLastModifiedDate)
        syncLastModifiedDate = try c.decodeIfPresent(Date.self, forKey: .syncLastModifiedDate)
        pendingAssetColumns = try c.decodeIfPresent([String].self, forKey: .pendingAssetColumns)
    }

    /// Whether the row has ever been confirmed with the server, in any format.
    var hasServerBase: Bool { syncedFingerprint != nil || ckRecordData != nil }

    /// Whether the metadata still uses an older format that should be rewritten.
    var needsUpgrade: Bool { ckRecordData != nil || systemFields != nil }

    var isEmpty: Bool {
        ckRecordData == nil && systemFields == nil && packedSystemFields == nil && syncedFingerprint == nil
            && (assetDigests?.isEmpty ?? true) && syncedLastModifiedDate == nil && syncLastModifiedDate == nil
            && (pendingAssetColumns?.isEmpty ?? true)
    }

    static func decode(_ data: Data?) -> SkybridgeMetadata {
        guard let data else { return SkybridgeMetadata() }
        return (try? PropertyListDecoder().decode(SkybridgeMetadata.self, from: data)) ?? SkybridgeMetadata()
    }

    func encoded() -> Data? {
        if isEmpty { return nil }
        let encoder = PropertyListEncoder()
        encoder.outputFormat = .binary
        return try? encoder.encode(self)
    }

    /// The last-synced record's system-fields archive, whichever format holds it.
    func systemFieldsArchive(recordType: String) -> Data? {
        if let packedSystemFields { return SkybridgeArchiveCodec.unpack(packedSystemFields, recordType: recordType) }
        return systemFields
    }

    /// Stores a system-fields archive in the current (compressed) format.
    mutating func setSystemFieldsArchive(_ archive: Data?, recordType: String) {
        systemFields = nil
        packedSystemFields = archive.flatMap { SkybridgeArchiveCodec.pack($0, recordType: recordType) }
        // If compression ever fails, keep the archive uncompressed rather than lose it.
        if archive != nil, packedSystemFields == nil { systemFields = archive }
    }

    /// Forgets the server base so the row uploads fresh as a new record.
    mutating func clearServerBase() {
        ckRecordData = nil
        systemFields = nil
        packedSystemFields = nil
        syncedFingerprint = nil
        assetDigests = nil
        syncedLastModifiedDate = nil
    }
}

extension Data {
    fileprivate init?(hex: String) {
        guard hex.count % 2 == 0 else { return nil }
        var data = Data(capacity: hex.count / 2)
        var index = hex.startIndex
        while index < hex.endIndex {
            let next = hex.index(index, offsetBy: 2)
            guard let byte = UInt8(hex[index..<next], radix: 16) else { return nil }
            data.append(byte)
            index = next
        }
        self = data
    }
}

/// Compresses system-fields archives against a template.
///
/// `NSKeyedArchiver` output for a record's system fields is almost entirely boilerplate that is
/// identical for every record of a type in a zone. Raw deflate with that boilerplate as a preset
/// dictionary (the archive of a placeholder record of the same type) shrinks a ~1.7 KB archive
/// to under 100 bytes. The template is deterministic, so it is rebuilt on demand and never stored.
internal enum SkybridgeArchiveCodec {
    private static let templates = Blackbird.Locked<[String: Data]>([:])

    static func template(recordType: String) -> Data {
        if let cached = templates.withLock({ $0[recordType] }) { return cached }
        let placeholder = CKRecord(recordType: recordType, recordID: CKRecord.ID(recordName: "template", zoneID: CKRecordZone.ID(zoneName: "Skybridge")))
        let archiver = NSKeyedArchiver(requiringSecureCoding: true)
        placeholder.encodeSystemFields(with: archiver)
        archiver.finishEncoding()
        let data = archiver.encodedData
        templates.withLock { $0[recordType] = data }
        return data
    }

    static func pack(_ data: Data, recordType: String) -> Data? {
        guard !data.isEmpty else { return nil }
        let dictionary = template(recordType: recordType)
        var stream = z_stream()
        guard deflateInit2_(&stream, Z_BEST_COMPRESSION, Z_DEFLATED, -15, 9, Z_DEFAULT_STRATEGY, ZLIB_VERSION, Int32(MemoryLayout<z_stream>.size)) == Z_OK else { return nil }
        defer { deflateEnd(&stream) }
        let dictionarySet = dictionary.withUnsafeBytes { deflateSetDictionary(&stream, $0.baseAddress?.assumingMemoryBound(to: Bytef.self), uInt(dictionary.count)) }
        guard dictionarySet == Z_OK else { return nil }
        var output = Data(count: Int(deflateBound(&stream, uLong(data.count))) + 64)
        let capacity = output.count
        var status: Int32 = Z_OK
        data.withUnsafeBytes { input in
            output.withUnsafeMutableBytes { out in
                stream.next_in = UnsafeMutablePointer(mutating: input.baseAddress!.assumingMemoryBound(to: Bytef.self))
                stream.avail_in = uInt(data.count)
                stream.next_out = out.baseAddress!.assumingMemoryBound(to: Bytef.self)
                stream.avail_out = uInt(capacity)
                status = deflate(&stream, Z_FINISH)
            }
        }
        guard status == Z_STREAM_END else { return nil }
        output.count = Int(stream.total_out)
        return output
    }

    static func unpack(_ data: Data, recordType: String) -> Data? {
        guard !data.isEmpty else { return nil }
        let dictionary = template(recordType: recordType)
        var stream = z_stream()
        guard inflateInit2_(&stream, -15, ZLIB_VERSION, Int32(MemoryLayout<z_stream>.size)) == Z_OK else { return nil }
        defer { inflateEnd(&stream) }
        let dictionarySet = dictionary.withUnsafeBytes { inflateSetDictionary(&stream, $0.baseAddress?.assumingMemoryBound(to: Bytef.self), uInt(dictionary.count)) }
        guard dictionarySet == Z_OK else { return nil }
        var output = Data()
        var chunk = [UInt8](repeating: 0, count: 8192)
        var status: Int32 = Z_OK
        data.withUnsafeBytes { input in
            stream.next_in = UnsafeMutablePointer(mutating: input.baseAddress!.assumingMemoryBound(to: Bytef.self))
            stream.avail_in = uInt(data.count)
            repeat {
                chunk.withUnsafeMutableBufferPointer { buffer in
                    stream.next_out = buffer.baseAddress
                    stream.avail_out = uInt(buffer.count)
                    status = inflate(&stream, Z_NO_FLUSH)
                    let produced = buffer.count - Int(stream.avail_out)
                    output.append(buffer.baseAddress!, count: produced)
                }
            } while status == Z_OK && output.count < 4 * 1024 * 1024
        }
        guard status == Z_STREAM_END else { return nil }
        return output
    }
}

/// Reads a `@BlackbirdColumn` wrapper's value without knowing its generic type.
private protocol SkybridgeColumnReading {
    var skybridgeAnyValue: Any { get }
}
extension BlackbirdColumn: SkybridgeColumnReading {
    fileprivate var skybridgeAnyValue: Any { value }
}

extension BlackbirdSkybridgeSyncable {
    // Helpers for Skybridge. Those without Self in their signatures are callable
    // on `any BlackbirdSkybridgeSyncable.Type` values without opening the existential.
    // Internal (not fileprivate) only where tests exercise them.

    internal static func skybridgeSyncedColumnNames() -> [String] {
        let excludedNames = Set(skybridgeExcludedColumns.map { columnInfoFromKeyPath($0).name })
        let primaryKeyNames = Set(primaryKey.map { columnInfoFromKeyPath($0).name })
        let assetNames = Set(skybridgeAssetColumnNames())
        return table.columns.map(\.name).filter {
            $0 != skybridgeMetadataColumnName && !excludedNames.contains($0) && !primaryKeyNames.contains($0) && !assetNames.contains($0)
        }
    }

    internal static func skybridgeAssetColumnNames() -> [String] {
        skybridgeAssetColumns.map { columnInfoFromKeyPath($0).name }
    }

    internal var skybridgeDecodedMetadata: SkybridgeMetadata { SkybridgeMetadata.decode(skybridgeMetadata) }

    internal mutating func setSkybridgeMetadata(_ metadata: SkybridgeMetadata) { skybridgeMetadata = metadata.encoded() }

    fileprivate static func skybridgePrimaryKeyInfo() -> Blackbird.ColumnInfo {
        guard primaryKey.count == 1, let keyPath = primaryKey.first else {
            fatalError("Skybridge: \(tableName) must declare a single-column primaryKey")
        }
        return columnInfoFromKeyPath(keyPath)
    }

    fileprivate static func skybridgeValidate() {
        if skybridgePrimaryKeyInfo().type == Data.self {
            fatalError("Skybridge: \(tableName) has an unsupported Data primary key")
        }
        if !table.columnNames.contains(skybridgeMetadataColumnName) {
            fatalError("Skybridge: \(tableName) must declare `@BlackbirdColumn var skybridgeMetadata: Data?`")
        }
        for keyPath in skybridgeAssetColumns {
            let info = columnInfoFromKeyPath(keyPath)
            // Optional columns report their type as Optional<Data>.
            if info.type != Data.self, info.type != Optional<Data>.self {
                fatalError("Skybridge: \(tableName).\(info.name) is declared as an asset column but isn't a Data column")
            }
        }
        _ = skybridgeSyncedColumnNames() // traps on excluded key-paths that aren't @BlackbirdColumn columns
    }

    fileprivate static func skybridgePrimaryKeyValue(fromString string: String) -> Blackbird.Value? {
        let type = skybridgePrimaryKeyInfo().type
        if type is any BlackbirdStorableAsInteger.Type { return Int64(string).map { .integer($0) } }
        if type is any BlackbirdStorableAsDouble.Type { return Double(string).map { .double($0) } }
        return .text(string)
    }

    fileprivate static func skybridgeChangeStream(in database: Blackbird.Database) -> AsyncStream<Blackbird.PrimaryKeyValues?> {
        let publisher = changePublisher(in: database)
        return AsyncStream { continuation in
            let cancellable = publisher.sink { change in
                continuation.yield(change.changedPrimaryKeys)
            }
            continuation.onTermination = { _ in cancellable.cancel() }
        }
    }
}

/// Persisted CKSyncEngine state so sync resumes across launches without refetching everything.
internal struct BlackbirdSkybridgeState: BlackbirdModel {
    @BlackbirdColumn var id: Int
    @BlackbirdColumn var stateSerialization: Data?

    static let singletonID = 0
}

/// Housekeeping that survives launches: when the last full reconcile ran, and whether every
/// row's metadata has been upgraded to the current format.
internal struct BlackbirdSkybridgeInfo: BlackbirdModel {
    @BlackbirdColumn var id: Int
    @BlackbirdColumn var lastFullReconcile: Date?
    @BlackbirdColumn var metadataUpgraded: Bool

    static let singletonID = 0
}

/// Rows written locally that haven't been reconciled with the server yet. Startup reconciles
/// only these instead of every row of every table.
internal struct BlackbirdSkybridgeDirtyRow: BlackbirdModel {
    static let primaryKey: [BlackbirdColumnKeyPath] = [ \.$recordName ]

    @BlackbirdColumn var recordName: String
    @BlackbirdColumn var tableName: String
    @BlackbirdColumn var primaryKeyString: String

    var id: String { recordName }
}

/// Record changes waiting to be handed to the sync engine. The engine's own pending list is
/// persisted whole on every state update, so bulk operations (first upload, purges) park their
/// changes here and feed the engine a slice at a time.
internal struct BlackbirdSkybridgeQueuedChange: BlackbirdModel {
    static let primaryKey: [BlackbirdColumnKeyPath] = [ \.$recordName ]

    @BlackbirdColumn var recordName: String
    @BlackbirdColumn var isDeletion: Bool

    var id: String { recordName }
}

/// Syncs ``BlackbirdSkybridgeSyncable`` tables to CloudKit via CKSyncEngine.
///
/// Sync is fully automatic and silent: conflicts resolve to the latest write,
/// and errors are logged and retried without ever involving the user.
///
/// Each row's metadata holds a fingerprint of its content as last confirmed with the server.
/// Comparing a row's own fingerprint against it distinguishes genuine local edits (which get
/// uploaded) from echoes of sync-applied writes (ignored), so no in-memory bookkeeping is
/// needed and devices can't ping-pong updates.
///
/// All rows sync as records in a single private "Skybridge" zone, with the record
/// type set to the table name and the record name set to "tableName:primaryKey".
@available(macOS 14, iOS 17, tvOS 17, watchOS 10, *)
public actor BlackbirdSkybridge {
    private enum Field {
        static let lastModified = "skybridge_lastModified"

        // Asset-column contents can't be compared through the record like
        // inline values (a last-synced archive's asset file may be gone), so
        // each asset column carries a digest of its contents in this sibling
        // encrypted field, compared instead.
        static func assetDigest(_ columnName: String) -> String { columnName + "_skybridgeAssetDigest" }
    }

    private static let logger = Logger(subsystem: Blackbird.loggingSubsystem, category: "Skybridge")

    /// How many changes the engine holds at once before the rest wait in the queue table.
    private static let engineSliceLimit = 2000
    /// Rows per transaction for bulk work.
    private static let batchSize = 400
    /// How often the paginated full reconcile runs.
    private static let fullReconcileInterval: TimeInterval = 24 * 3600

    private let zoneID = CKRecordZone.ID(zoneName: "Skybridge")
    private let syncedTypes: [any BlackbirdSkybridgeSyncable.Type]

    private var models: [String: any BlackbirdSkybridgeSyncable.Type] = [:] // keyed by tableName == CKRecord type
    private var database: Blackbird.Database?
    private var engine: CKSyncEngine?
    private var localChangeTasks: [Task<Void, Never>] = []
    private var isToppingUp = false

    // Deletions this engine has just applied locally, keyed by record name. The local
    // deletion echoes back through the change stream as a missing row, which must not
    // be sent to the server as a new deletion: an unconditional delete could destroy
    // a record that another device legitimately recreated in the meantime.
    private var recentServerDeletions: [String: Date] = [:]

    private func noteServerDeletion(recordName: String) {
        recentServerDeletions[recordName] = Date()
    }

    private func consumeServerDeletion(recordName: String) -> Bool {
        let cutoff = Date(timeIntervalSinceNow: -300)
        recentServerDeletions = recentServerDeletions.filter { $0.value > cutoff }
        return recentServerDeletions.removeValue(forKey: recordName) != nil
    }

    public init(syncing syncedTypes: [any BlackbirdSkybridgeSyncable.Type]) {
        self.syncedTypes = syncedTypes
    }

    /// Begins syncing. The database must already have resolved the schemas of every synced type.
    public func start(database: Blackbird.Database) async {
        guard self.database == nil else { return }
        self.database = database

        try? await BlackbirdSkybridgeState.resolveSchema(in: database)
        try? await BlackbirdSkybridgeInfo.resolveSchema(in: database)
        try? await BlackbirdSkybridgeDirtyRow.resolveSchema(in: database)
        try? await BlackbirdSkybridgeQueuedChange.resolveSchema(in: database)

        for type in syncedTypes {
            type.skybridgeValidate()
            models[type.tableName] = type
        }

        var stateSerialization: CKSyncEngine.State.Serialization? = nil
        if let savedState = try? await BlackbirdSkybridgeState.read(from: database, id: BlackbirdSkybridgeState.singletonID),
           let data = savedState.stateSerialization {
            stateSerialization = try? JSONDecoder().decode(CKSyncEngine.State.Serialization.self, from: data)
        }

        let configuration = CKSyncEngine.Configuration(
            database: CKContainer.default().privateCloudDatabase,
            stateSerialization: stateSerialization,
            delegate: self
        )
        let engine = CKSyncEngine(configuration)
        self.engine = engine
        Self.logger.info("☁️ Skybridge started: \(self.models.keys.sorted().joined(separator: ", "), privacy: .public)")

        Task.detached(priority: .background) { Self.cleanUpStagedAssets() }

        if stateSerialization == nil {
            // First run (or state was lost): create the zone and upload everything.
            await scheduleFullUpload(clearingServerRecords: false)
        }

        for type in syncedTypes {
            let tableName = type.tableName
            localChangeTasks.append(Task { [weak self] in
                for await primaryKeys in type.skybridgeChangeStream(in: database) {
                    await self?.handleLocalChange(tableName: tableName, primaryKeys: primaryKeys)
                }
            })
        }

        if stateSerialization != nil {
            // Rows written since their last reconcile (e.g. the app was killed mid-sync).
            await reconcileDirtyRows()
        }
        await topUpPendingChanges()

        Task(priority: .background) { [weak self] in
            await self?.runHousekeeping()
        }
    }

    /// Slow, resumable work that never blocks the app: upgrading metadata left in the original
    /// format, retrying pending assets, and the periodic full reconcile.
    private func runHousekeeping() async {
        guard let database else { return }
        let info = (try? await BlackbirdSkybridgeInfo.read(from: database, id: BlackbirdSkybridgeInfo.singletonID))
            ?? BlackbirdSkybridgeInfo(id: BlackbirdSkybridgeInfo.singletonID, lastFullReconcile: nil, metadataUpgraded: false)

        if !info.metadataUpgraded {
            for model in models.values { await upgradeMetadata(model) }
            try? await BlackbirdSkybridgeInfo(id: BlackbirdSkybridgeInfo.singletonID, lastFullReconcile: info.lastFullReconcile, metadataUpgraded: true).write(to: database)
            Self.logger.info("☁️ Skybridge metadata upgraded to the current format")
        }

        await refetchRecordsWithPendingAssets()

        if (info.lastFullReconcile ?? .distantPast) < Date(timeIntervalSinceNow: -Self.fullReconcileInterval) {
            for model in models.values { await reconcileAllRows(model) }
            try? await BlackbirdSkybridgeInfo(id: BlackbirdSkybridgeInfo.singletonID, lastFullReconcile: Date(), metadataUpgraded: true).write(to: database)
        }
    }

    // MARK: - Record identity

    private func recordID(tableName: String, primaryKeyString: String) -> CKRecord.ID {
        CKRecord.ID(recordName: "\(tableName):\(primaryKeyString)", zoneID: zoneID)
    }

    private func recordName(tableName: String, primaryKeyString: String) -> String {
        "\(tableName):\(primaryKeyString)"
    }

    /// Resolves a record ID back to its model type and primary-key value. Record
    /// names are "tableName:primaryKey"; table names never contain a colon.
    private func modelAndPrimaryKey(for recordID: CKRecord.ID) -> (model: any BlackbirdSkybridgeSyncable.Type, primaryKey: Blackbird.Value)? {
        modelAndPrimaryKey(forRecordName: recordID.recordName)
    }

    private func modelAndPrimaryKey(forRecordName name: String) -> (model: any BlackbirdSkybridgeSyncable.Type, primaryKey: Blackbird.Value)? {
        guard let separator = name.firstIndex(of: ":"), let model = models[String(name[name.startIndex..<separator])] else { return nil }
        let primaryKeyString = String(name[name.index(after: separator)...])
        guard let primaryKey = model.skybridgePrimaryKeyValue(fromString: primaryKeyString) else { return nil }
        return (model, primaryKey)
    }

    // MARK: - Pending changes
    //
    // The engine's pending list is serialized whole on every state update, so it is kept to a
    // bounded slice; everything beyond that waits in the queue table and is fed in as sends
    // complete. Changes that fit go straight to the engine.

    private func enqueue(_ changes: [CKSyncEngine.PendingRecordZoneChange]) async {
        guard let engine, let database, !changes.isEmpty else { return }
        let room = max(0, Self.engineSliceLimit - engine.state.pendingRecordZoneChanges.count)
        let direct = Array(changes.prefix(room))
        let queued = Array(changes.dropFirst(room))
        if !direct.isEmpty { engine.state.add(pendingRecordZoneChanges: direct) }
        if !queued.isEmpty {
            let rows = queued.map { change -> BlackbirdSkybridgeQueuedChange in
                switch change {
                    case .saveRecord(let id): return BlackbirdSkybridgeQueuedChange(recordName: id.recordName, isDeletion: false)
                    case .deleteRecord(let id): return BlackbirdSkybridgeQueuedChange(recordName: id.recordName, isDeletion: true)
                    @unknown default: return BlackbirdSkybridgeQueuedChange(recordName: "", isDeletion: false)
                }
            }.filter { !$0.recordName.isEmpty }
            for chunk in rows.chunked(into: Self.batchSize) {
                try? await database.transaction { core in
                    for row in chunk { try row.write(to: core) }
                }
            }
        }
    }

    /// Moves queued changes into the engine while it has room.
    private func topUpPendingChanges() async {
        guard let engine, let database, !isToppingUp else { return }
        isToppingUp = true
        defer { isToppingUp = false }
        while engine.state.pendingRecordZoneChanges.count < Self.engineSliceLimit {
            let room = Self.engineSliceLimit - engine.state.pendingRecordZoneChanges.count
            let limit = min(room, Self.batchSize)
            guard let rows = try? await BlackbirdSkybridgeQueuedChange.read(from: database, sqlWhere: "1 ORDER BY rowid LIMIT \(limit)"), !rows.isEmpty else { return }
            let changes: [CKSyncEngine.PendingRecordZoneChange] = rows.map {
                let id = CKRecord.ID(recordName: $0.recordName, zoneID: zoneID)
                return $0.isDeletion ? .deleteRecord(id) : .saveRecord(id)
            }
            engine.state.add(pendingRecordZoneChanges: changes)
            try? await database.transaction { core in
                for row in rows { try row.delete(from: core) }
            }
        }
    }

    // MARK: - Local changes

    private func handleLocalChange(tableName: String, primaryKeys: Blackbird.PrimaryKeyValues?) async {
        guard let model = models[tableName] else { return }

        if let primaryKeys {
            let keys = primaryKeys.compactMap(\.first)
            await markDirty(model, primaryKeys: keys)
            await reconcileRows(model, primaryKeys: keys)
        } else {
            // Unknown scope of changes: re-check every row.
            await reconcileAllRows(model)
        }
    }

    /// Records touched keys before reconciling them, so a crash mid-way leaves them for the
    /// next launch instead of the next daily pass.
    private func markDirty<T: BlackbirdSkybridgeSyncable>(_ type: T.Type, primaryKeys: [Blackbird.Value]) async {
        guard let database else { return }
        let rows = primaryKeys.compactMap { key -> BlackbirdSkybridgeDirtyRow? in
            guard let keyString = key.stringValue else { return nil }
            return BlackbirdSkybridgeDirtyRow(recordName: recordName(tableName: T.tableName, primaryKeyString: keyString), tableName: T.tableName, primaryKeyString: keyString)
        }
        for chunk in rows.chunked(into: Self.batchSize) {
            try? await database.transaction { core in
                for row in chunk { try row.write(to: core) }
            }
        }
    }

    private func clearDirty(recordNames: [String]) async {
        guard let database, !recordNames.isEmpty else { return }
        for chunk in recordNames.chunked(into: Self.batchSize) {
            try? await database.transaction { core in
                for name in chunk {
                    if let row = try BlackbirdSkybridgeDirtyRow.read(from: core, primaryKey: name) { try row.delete(from: core) }
                }
            }
        }
    }

    private func reconcileDirtyRows() async {
        guard let database else { return }
        let dirty = (try? await BlackbirdSkybridgeDirtyRow.read(from: database, matching: .all)) ?? []
        guard !dirty.isEmpty else { return }
        Self.logger.info("☁️ Skybridge reconciling \(dirty.count) rows left dirty")
        let byTable = Dictionary(grouping: dirty, by: \.tableName)
        for (tableName, rows) in byTable {
            guard let model = models[tableName] else {
                await clearDirty(recordNames: rows.map(\.recordName))
                continue
            }
            let keys = rows.compactMap { model.skybridgePrimaryKeyValue(fromString: $0.primaryKeyString) }
            await reconcileRows(model, primaryKeys: keys)
        }
    }

    /// Checks a set of rows cheaply (one bulk read, fingerprints in memory) and only takes a
    /// transaction for rows that actually differ from their server base or are missing.
    private func reconcileRows<T: BlackbirdSkybridgeSyncable>(_ type: T.Type, primaryKeys: [Blackbird.Value]) async {
        guard let database, !primaryKeys.isEmpty else { return }
        var changed: [Blackbird.Value] = []
        var cleanNames: [String] = []
        for chunk in primaryKeys.chunked(into: Self.batchSize) {
            let instances = (try? await T.read(from: database, primaryKeys: chunk)) ?? []
            var found = Set<Blackbird.Value>()
            for instance in instances {
                let key = Self.primaryKeyValue(of: instance)
                found.insert(key)
                if Self.isClean(instance) {
                    if let keyString = key.stringValue { cleanNames.append(recordName(tableName: T.tableName, primaryKeyString: keyString)) }
                } else {
                    changed.append(key)
                }
            }
            changed += chunk.filter { !found.contains($0) } // missing rows: deleted locally
        }
        await clearDirty(recordNames: cleanNames)
        for key in changed {
            await reconcileLocalRow(type, primaryKey: key)
        }
    }

    /// Whether a row matches its server base in the current metadata format, without a transaction.
    private static func isClean<T: BlackbirdSkybridgeSyncable>(_ instance: T) -> Bool {
        let metadata = instance.skybridgeDecodedMetadata
        guard !metadata.needsUpgrade, let fingerprint = metadata.syncedFingerprint else { return false }
        return localFingerprint(of: instance, metadata: metadata) == fingerprint
    }

    /// Walks a whole table in primary-key pages, comparing fingerprints in memory and sending only
    /// the rows that differ through the transactional path. Yields between pages.
    private func reconcileAllRows<T: BlackbirdSkybridgeSyncable>(_ type: T.Type) async {
        guard let database else { return }
        var lastRowID: Int64 = 0
        while true {
            let rows = (try? await T.query(in: database, "SELECT rowid AS __rowid, * FROM `\(T.tableName)` WHERE rowid > ? ORDER BY rowid LIMIT \(Self.batchSize)", lastRowID)) ?? []
            guard !rows.isEmpty else { return }
            var changed: [Blackbird.Value] = []
            for row in rows {
                guard let rowID = row["__rowid"]?.int64Value else { continue }
                lastRowID = max(lastRowID, rowID)
                var plain = row.row
                plain.removeValue(forKey: "__rowid")
                guard let instance = try? T.instance(from: plain, in: database) else { continue }
                if !Self.isClean(instance) { changed.append(Self.primaryKeyValue(of: instance)) }
            }
            for key in changed { await reconcileLocalRow(type, primaryKey: key) }
            await Task.yield()
        }
    }

    /// Rewrites rows still carrying a full-record archive into the current format, in pages.
    private func upgradeMetadata<T: BlackbirdSkybridgeSyncable>(_ type: T.Type) async {
        guard let database else { return }
        var lastRowID: Int64 = 0
        while true {
            let rows = (try? await T.query(in: database, "SELECT rowid AS __rowid, `\(Self.primaryKeyName(T.self))` AS __pk, `\(skybridgeMetadataColumnName)` AS __meta FROM `\(T.tableName)` WHERE rowid > ? ORDER BY rowid LIMIT \(Self.batchSize)", lastRowID)) ?? []
            guard !rows.isEmpty else { return }
            var keys: [Blackbird.Value] = []
            for row in rows {
                guard let rowID = row["__rowid"]?.int64Value else { continue }
                lastRowID = max(lastRowID, rowID)
                if case .data(let data) = row["__meta"] ?? .null, SkybridgeMetadata.decode(data).needsUpgrade, let key = row["__pk"] { keys.append(key) }
            }
            if !keys.isEmpty {
                let batch = keys
                try? await database.transaction { core in
                    for key in batch {
                        _ = try? T.modify(in: core, primaryKey: key) { _, instance in
                            var metadata = instance.skybridgeDecodedMetadata
                            if Self.upgrade(&metadata, for: T.self) { instance.setSkybridgeMetadata(metadata) }
                        }
                    }
                }
            }
            await Task.yield()
        }
    }

    /// Converts original-format metadata in place. Returns whether anything changed.
    internal static func upgrade<T: BlackbirdSkybridgeSyncable>(_ metadata: inout SkybridgeMetadata, for type: T.Type) -> Bool {
        guard metadata.needsUpgrade else { return false }
        if let archive = metadata.ckRecordData {
            if let record = decodeRecord(archive) {
                metadata.setSystemFieldsArchive(encodeSystemFields(record), recordType: T.tableName)
                metadata.syncedFingerprint = recordFingerprint(record, for: T.self)
                metadata.assetDigests = assetDigests(from: record, for: T.self)
                metadata.syncedLastModifiedDate = (record.encryptedValues[Field.lastModified] as? Date) ?? record.modificationDate
            }
            // An unreadable archive is dropped: the row re-uploads as new, which is the safe outcome.
            metadata.ckRecordData = nil
        } else if let raw = metadata.systemFields {
            // Interim format: just compress the archive.
            metadata.setSystemFieldsArchive(raw, recordType: T.tableName)
        }
        return true
    }

    private enum ReconcileAction: Sendable {
        case ignore, upload, deleteFromServer
    }

    /// Compares a row against its server base and schedules an upload (or a deletion) if they
    /// differ. Rows that match are echoes of changes this engine just applied, and are ignored.
    private func reconcileLocalRow<T: BlackbirdSkybridgeSyncable>(_ type: T.Type, primaryKey: Blackbird.Value) async {
        guard let database, let primaryKeyString = primaryKey.stringValue else { return }
        let recordName = recordName(tableName: T.tableName, primaryKeyString: primaryKeyString)
        let recordID = CKRecord.ID(recordName: recordName, zoneID: zoneID)

        // The read, comparison, and timestamp bump happen in one transaction so a
        // concurrent writer can't be overwritten by a stale full-row rewrite.
        let action: ReconcileAction = (try? await database.transaction { core in
            guard var instance = try T.read(from: core, primaryKey: primaryKey) else { return ReconcileAction.deleteFromServer }

            var metadata = instance.skybridgeDecodedMetadata
            var metadataChanged = Self.upgrade(&metadata, for: T.self)
            if let fingerprint = metadata.syncedFingerprint, Self.localFingerprint(of: instance, metadata: metadata) == fingerprint {
                if metadataChanged { instance.setSkybridgeMetadata(metadata); try instance.write(to: core) }
                return ReconcileAction.ignore
            }

            // A genuine local edit: bump the last-write-wins timestamp if the
            // writer didn't. (The rewrite re-enters here once, then no-ops.)
            let lastSyncedModified = metadata.syncedLastModifiedDate
            let localModified = metadata.syncLastModifiedDate
            if localModified == nil || (lastSyncedModified != nil && localModified! <= lastSyncedModified!) {
                metadata.syncLastModifiedDate = Date()
                metadataChanged = true
            }
            if metadataChanged {
                instance.setSkybridgeMetadata(metadata)
                try instance.write(to: core)
            }
            return ReconcileAction.upload
        }) ?? .ignore // a failed read/write here must not become a server deletion

        switch action {
            case .ignore:
                await clearDirty(recordNames: [recordName])
            case .upload:
                await enqueue([.saveRecord(recordID)])
                await clearDirty(recordNames: [recordName])
            case .deleteFromServer:
                if !consumeServerDeletion(recordName: recordName) {
                    await enqueue([.deleteRecord(recordID)])
                }
                await clearDirty(recordNames: [recordName])
        }
    }

    // MARK: - Server changes

    /// A fetched record reduced to Sendable parts, so it can be applied inside a transaction.
    private struct ServerRecord: Sendable {
        let recordName: String
        let primaryKey: Blackbird.Value
        let serverModified: Date
        let systemFields: Data?
        let fingerprint: Data
        let assetDigests: [String: String]
        let values: [(name: String, value: Blackbird.Value)]
        let pendingAssetColumns: [String]
    }

    private func serverRecord<T: BlackbirdSkybridgeSyncable>(from record: CKRecord, for type: T.Type, primaryKey: Blackbird.Value) -> ServerRecord {
        var values: [(name: String, value: Blackbird.Value)] = T.skybridgeSyncedColumnNames().map {
            ($0, Self.blackbirdValue(from: record.encryptedValues[$0]))
        }
        // An asset the engine failed to materialize is applied as *pending*:
        // the record's inline fields (e.g. an app-level has-asset flag) land
        // immediately, the column goes NULL, and the pending bookkeeping keeps
        // the row from reading as a local deletion — which would upload the
        // NULL and destroy the server's asset. A refetch retries the download.
        var pending: [String] = []
        for name in T.skybridgeAssetColumnNames() {
            guard let asset = record[name] as? CKAsset else {
                values.append((name, .null))
                continue
            }
            if let fileURL = asset.fileURL, let data = try? Data(contentsOf: fileURL) {
                values.append((name, .data(data)))
            } else {
                Self.logger.error("☁️⌛ Skybridge: cannot read fetched asset \(name, privacy: .public) of \(record.recordID.recordName, privacy: .public); applying as pending")
                values.append((name, .null))
                pending.append(name)
            }
        }
        return ServerRecord(
            recordName: record.recordID.recordName,
            primaryKey: primaryKey,
            serverModified: record.encryptedValues[Field.lastModified] as? Date ?? record.modificationDate ?? .distantPast,
            systemFields: Self.encodeSystemFields(record),
            fingerprint: Self.recordFingerprint(record, for: T.self),
            assetDigests: Self.assetDigests(from: record, for: T.self),
            values: values,
            pendingAssetColumns: pending
        )
    }

    /// Applies fetched records in one transaction per batch, keeping the latest write of each:
    /// if the local row was modified more recently, its values win and are re-uploaded on top
    /// of the server record; otherwise the server values are applied locally.
    private func applyServerRecords<T: BlackbirdSkybridgeSyncable>(_ type: T.Type, records: [ServerRecord]) async {
        guard let database, !records.isEmpty else { return }
        for chunk in records.chunked(into: Self.batchSize) {
            let outcome: (localWins: [String], pendingRefetch: [String]) = (try? await database.transaction { core in
                var localWins: [String] = []
                var pendingRefetch: [String] = []
                for server in chunk {
                    let existing = try T.read(from: core, primaryKey: server.primaryKey)

                    if var existing {
                        var metadata = existing.skybridgeDecodedMetadata
                        if let localModified = metadata.syncLastModifiedDate, localModified > server.serverModified {
                            // Local wins: keep local values, adopt the server record as the new base, re-send.
                            metadata.ckRecordData = nil
                            metadata.setSystemFieldsArchive(server.systemFields, recordType: T.tableName)
                            metadata.syncedFingerprint = server.fingerprint
                            metadata.assetDigests = server.assetDigests.isEmpty ? nil : server.assetDigests
                            metadata.syncedLastModifiedDate = server.serverModified
                            existing.setSkybridgeMetadata(metadata)
                            try existing.write(to: core)
                            localWins.append(server.recordName)
                            continue
                        }
                    }

                    // Server wins (or no local row): overlay the server values onto the row and write it back.
                    var row: Blackbird.Row = existing.map { Self.rowRepresentation(of: $0) } ?? [:]
                    row[Self.primaryKeyName(T.self)] = server.primaryKey
                    for (name, value) in server.values { row[name] = value }
                    var metadata = SkybridgeMetadata()
                    metadata.setSystemFieldsArchive(server.systemFields, recordType: T.tableName)
                    metadata.syncedFingerprint = server.fingerprint
                    metadata.assetDigests = server.assetDigests.isEmpty ? nil : server.assetDigests
                    metadata.syncLastModifiedDate = server.serverModified
                    metadata.syncedLastModifiedDate = server.serverModified
                    metadata.pendingAssetColumns = server.pendingAssetColumns.isEmpty ? nil : server.pendingAssetColumns
                    row[skybridgeMetadataColumnName] = metadata.encoded().map { .data($0) } ?? .null

                    let updated = try T.instance(from: row, in: database)
                    try updated.write(to: core)
                    if !server.pendingAssetColumns.isEmpty { pendingRefetch.append(server.recordName) }
                }
                return (localWins, pendingRefetch)
            }) ?? ([], [])

            if !outcome.localWins.isEmpty {
                await enqueue(outcome.localWins.map { .saveRecord(CKRecord.ID(recordName: $0, zoneID: zoneID)) })
            }
            for name in outcome.pendingRefetch { scheduleAssetRefetch(recordID: CKRecord.ID(recordName: name, zoneID: zoneID)) }
        }
    }

    private func applyServerRecord(_ record: CKRecord) async {
        guard let (model, primaryKey) = modelAndPrimaryKey(for: record.recordID) else { return }
        await applyFetchedRecords(model, records: [record], primaryKeys: [primaryKey])
    }

    private func applyFetchedRecords<T: BlackbirdSkybridgeSyncable>(_ type: T.Type, records: [CKRecord], primaryKeys: [Blackbird.Value]) async {
        let extracted = zip(records, primaryKeys).map { serverRecord(from: $0, for: T.self, primaryKey: $1) }
        await applyServerRecords(T.self, records: extracted)
    }

    private func handleFetchedRecordZoneChanges(_ changes: CKSyncEngine.Event.FetchedRecordZoneChanges) async {
        // Group by table so each batch goes through one generic, one transaction.
        var byTable: [String: (records: [CKRecord], keys: [Blackbird.Value])] = [:]
        for modification in changes.modifications {
            guard let (model, primaryKey) = modelAndPrimaryKey(for: modification.record.recordID) else { continue }
            byTable[model.tableName, default: ([], [])].records.append(modification.record)
            byTable[model.tableName, default: ([], [])].keys.append(primaryKey)
        }
        for (tableName, group) in byTable {
            guard let model = models[tableName] else { continue }
            await applyFetchedRecords(model, records: group.records, primaryKeys: group.keys)
        }

        var deletionsByTable: [String: [(recordName: String, key: Blackbird.Value)]] = [:]
        for deletion in changes.deletions {
            guard let (model, primaryKey) = modelAndPrimaryKey(for: deletion.recordID) else { continue }
            deletionsByTable[model.tableName, default: []].append((deletion.recordID.recordName, primaryKey))
        }
        for (tableName, deletions) in deletionsByTable {
            guard let model = models[tableName] else { continue }
            await applyServerDeletions(model, deletions: deletions)
        }
    }

    private func applyServerDeletions<T: BlackbirdSkybridgeSyncable>(_ type: T.Type, deletions: [(recordName: String, key: Blackbird.Value)]) async {
        guard let database else { return }
        for chunk in deletions.chunked(into: Self.batchSize) {
            for deletion in chunk { noteServerDeletion(recordName: deletion.recordName) }
            try? await database.transaction { core in
                for deletion in chunk {
                    if let instance = try T.read(from: core, primaryKey: deletion.key) { try instance.delete(from: core) }
                }
            }
        }
    }

    // MARK: - Pending-asset refetch
    //
    // The sync engine delivers each server record once; if its asset couldn't
    // be read then, nothing would ever retry. Pending assets are retried by
    // fetching the record directly: once shortly after the failure, and again
    // at every start for anything still pending.

    /// Names already retried this launch, so a persistently failing asset
    /// retries once per launch instead of looping.
    private var refetchedRecordNames: Set<String> = []

    private func scheduleAssetRefetch(recordID: CKRecord.ID, afterSeconds: Double = 30) {
        guard !refetchedRecordNames.contains(recordID.recordName) else { return }
        refetchedRecordNames.insert(recordID.recordName)
        Task { [weak self] in
            try? await Task.sleep(nanoseconds: UInt64(afterSeconds * 1_000_000_000))
            await self?.refetchRecord(recordID: recordID)
        }
    }

    private func refetchRecord(recordID: CKRecord.ID) async {
        guard let record = try? await CKContainer.default().privateCloudDatabase.record(for: recordID) else { return }
        await applyServerRecord(record)
    }

    private func refetchRecordsWithPendingAssets() async {
        for model in models.values {
            await refetchRecordsWithPendingAssets(model)
        }
    }

    private func refetchRecordsWithPendingAssets<T: BlackbirdSkybridgeSyncable>(_ type: T.Type) async {
        guard let database, !T.skybridgeAssetColumns.isEmpty else { return }
        var lastRowID: Int64 = 0
        while true {
            let rows = (try? await T.query(in: database, "SELECT rowid AS __rowid, `\(Self.primaryKeyName(T.self))` AS __pk, `\(skybridgeMetadataColumnName)` AS __meta FROM `\(T.tableName)` WHERE rowid > ? ORDER BY rowid LIMIT \(Self.batchSize)", lastRowID)) ?? []
            guard !rows.isEmpty else { return }
            for row in rows {
                guard let rowID = row["__rowid"]?.int64Value else { continue }
                lastRowID = max(lastRowID, rowID)
                guard case .data(let data) = row["__meta"] ?? .null,
                      let pending = SkybridgeMetadata.decode(data).pendingAssetColumns, !pending.isEmpty,
                      let keyString = row["__pk"]?.stringValue else { continue }
                await refetchRecord(recordID: recordID(tableName: T.tableName, primaryKeyString: keyString))
            }
            await Task.yield()
        }
    }

    /// Records a successful save: the saved record becomes the row's server base.
    /// - Returns: Whether the row exists.
    @discardableResult
    private func storeServerBase<T: BlackbirdSkybridgeSyncable>(_ type: T.Type, primaryKey: Blackbird.Value, savedRecord: CKRecord?) async -> Bool {
        guard let database else { return false }
        let systemFields = savedRecord.flatMap { Self.encodeSystemFields($0) }
        let fingerprint = savedRecord.map { Self.recordFingerprint($0, for: T.self) }
        let digests = savedRecord.map { Self.assetDigests(from: $0, for: T.self) } ?? [:]
        let syncedModified = savedRecord.flatMap { ($0.encryptedValues[Field.lastModified] as? Date) ?? $0.modificationDate }
        do {
            try await T.modify(in: database, primaryKey: primaryKey) { _, instance in
                var metadata = instance.skybridgeDecodedMetadata
                metadata.ckRecordData = nil
                metadata.setSystemFieldsArchive(systemFields, recordType: T.tableName)
                metadata.syncedFingerprint = fingerprint
                metadata.assetDigests = digests.isEmpty ? nil : digests
                metadata.syncedLastModifiedDate = syncedModified
                // Columns that now hold local bytes are no longer awaiting a
                // download; NULL columns stay pending until a refetch succeeds.
                if let pending = metadata.pendingAssetColumns, !pending.isEmpty {
                    let row = Self.rowRepresentation(of: instance)
                    let stillPending = pending.filter { name in
                        if case .data = row[name] ?? .null { return false }
                        return true
                    }
                    metadata.pendingAssetColumns = stillPending.isEmpty ? nil : stillPending
                }
                instance.setSkybridgeMetadata(metadata)
            }
            return true
        } catch {
            return false
        }
    }

    private func handleSentRecordZoneChanges(_ event: CKSyncEngine.Event.SentRecordZoneChanges) async {
        guard let engine else { return }

        for record in event.savedRecords {
            guard let (model, primaryKey) = modelAndPrimaryKey(for: record.recordID) else { continue }
            await storeServerBase(model, primaryKey: primaryKey, savedRecord: record)
        }

        for failedSave in event.failedRecordSaves {
            let recordID = failedSave.record.recordID
            switch failedSave.error.code {
                case .serverRecordChanged:
                    if let serverRecord = failedSave.error.serverRecord {
                        await applyServerRecord(serverRecord)
                    }
                case .zoneNotFound:
                    engine.state.add(pendingDatabaseChanges: [.saveZone(CKRecordZone(zoneID: zoneID))])
                    await clearServerRecordAndResend(recordID: recordID)
                case .unknownItem:
                    // Deleted on the server, but our save is the latest write: recreate it.
                    await clearServerRecordAndResend(recordID: recordID)
                case .batchRequestFailed, .quotaExceeded:
                    // Not retried automatically, and failed saves are removed from the pending
                    // queue. batchRequestFailed just means another record in the atomic batch
                    // failed; quotaExceeded records send once the account has space again.
                    engine.state.add(pendingRecordZoneChanges: [.saveRecord(recordID)])
                case .networkFailure, .networkUnavailable, .zoneBusy, .serviceUnavailable, .notAuthenticated, .operationCancelled, .requestRateLimited:
                    break // Transient; the sync engine retries automatically.
                default:
                    Self.logger.error("☁️🛑 Skybridge: unhandled save failure for \(recordID.recordName, privacy: .public): \(failedSave.error, privacy: .public)")
            }
        }

        // Failed deletes (e.g. records already gone from the server) need no recovery.
        await topUpPendingChanges()
    }

    /// Discards stale server metadata for a row and schedules a fresh upload.
    private func clearServerRecordAndResend(recordID: CKRecord.ID) async {
        guard let (model, primaryKey) = modelAndPrimaryKey(for: recordID) else { return }
        if await storeServerBase(model, primaryKey: primaryKey, savedRecord: nil) {
            await enqueue([.saveRecord(recordID)])
        }
    }

    // MARK: - Account and zone resets

    private func handleAccountChange(_ event: CKSyncEngine.Event.AccountChange) async {
        switch event.changeType {
            case .signIn:
                await scheduleFullUpload(clearingServerRecords: false)
            case .signOut, .switchAccounts:
                // Server records belong to the old account; discard their
                // metadata so everything re-uploads cleanly when possible.
                await scheduleFullUpload(clearingServerRecords: true)
            @unknown default:
                break
        }
    }

    private func handleFetchedDatabaseChanges(_ changes: CKSyncEngine.Event.FetchedDatabaseChanges) async {
        for deletion in changes.deletions where deletion.zoneID == zoneID {
            // The zone was deleted, purged, or reset on the server:
            // recreate it and re-upload everything.
            await scheduleFullUpload(clearingServerRecords: true)
        }
    }

    private func scheduleFullUpload(clearingServerRecords: Bool) async {
        guard let engine else { return }
        engine.state.add(pendingDatabaseChanges: [.saveZone(CKRecordZone(zoneID: zoneID))])
        for model in models.values {
            await scheduleFullUpload(model, clearingServerRecords: clearingServerRecords)
        }
        await topUpPendingChanges()
    }

    /// Queues every row of a table for upload, in pages, without loading the table into memory.
    private func scheduleFullUpload<T: BlackbirdSkybridgeSyncable>(_ type: T.Type, clearingServerRecords: Bool) async {
        guard let database else { return }
        var lastRowID: Int64 = 0
        while true {
            let rows = (try? await T.query(in: database, "SELECT rowid AS __rowid, `\(Self.primaryKeyName(T.self))` AS __pk FROM `\(T.tableName)` WHERE rowid > ? ORDER BY rowid LIMIT \(Self.batchSize)", lastRowID)) ?? []
            guard !rows.isEmpty else { return }
            var keys: [Blackbird.Value] = []
            for row in rows {
                guard let rowID = row["__rowid"]?.int64Value, let key = row["__pk"] else { continue }
                lastRowID = max(lastRowID, rowID)
                keys.append(key)
            }
            if clearingServerRecords {
                let batch = keys
                try? await database.transaction { core in
                    for key in batch {
                        _ = try? T.modify(in: core, primaryKey: key) { _, instance in
                            var metadata = instance.skybridgeDecodedMetadata
                            guard metadata.hasServerBase else { return }
                            metadata.clearServerBase()
                            instance.setSkybridgeMetadata(metadata)
                        }
                    }
                }
            }
            await enqueue(keys.compactMap { key in
                key.stringValue.map { .saveRecord(recordID(tableName: T.tableName, primaryKeyString: $0)) }
            })
            await Task.yield()
        }
    }

    // MARK: - State persistence

    private func persistState(_ serialization: CKSyncEngine.State.Serialization) async {
        guard let database else { return }
        let data = try? JSONEncoder().encode(serialization)
        try? await BlackbirdSkybridgeState(id: BlackbirdSkybridgeState.singletonID, stateSerialization: data).write(to: database)
    }

    // MARK: - Record conversion

    private func recordForUpload<T: BlackbirdSkybridgeSyncable>(_ type: T.Type, primaryKey: Blackbird.Value, recordID: CKRecord.ID) async -> CKRecord? {
        guard let database else { return nil }
        // A missing last-write-wins timestamp (e.g. rows queued by a full upload) is
        // assigned and persisted here, before it's stamped into the uploaded record:
        // otherwise the post-save echo check would see a mismatch and re-upload every row.
        // Original-format metadata is upgraded on the way.
        let instance: T? = try? await T.modify(in: database, primaryKey: primaryKey) { _, instance in
            var metadata = instance.skybridgeDecodedMetadata
            var changed = Self.upgrade(&metadata, for: T.self)
            if metadata.syncLastModifiedDate == nil {
                metadata.syncLastModifiedDate = Date()
                changed = true
            }
            if changed { instance.setSkybridgeMetadata(metadata) }
            return instance
        }
        guard let instance else { return nil }
        return Self.record(for: instance, recordID: recordID)
    }

    private static func record<T: BlackbirdSkybridgeSyncable>(for instance: T, recordID: CKRecord.ID) -> CKRecord {
        let metadata = instance.skybridgeDecodedMetadata
        let record = metadata.systemFieldsArchive(recordType: T.tableName).flatMap { decodeSystemFields($0) } ?? CKRecord(recordType: T.tableName, recordID: recordID)
        let row = rowRepresentation(of: instance)
        for name in T.skybridgeSyncedColumnNames() {
            setValue(row[name] ?? .null, on: record, forKey: name)
        }
        let pendingColumns = Set(metadata.pendingAssetColumns ?? [])
        for name in T.skybridgeAssetColumnNames() {
            if case .data(let data) = row[name] ?? .null {
                // Rewritten from the column only when the bytes actually
                // changed: setting the field marks it changed, and an
                // unchanged multi-megabyte asset re-uploading on every save
                // slows the whole sync down.
                let digest = assetDigest(data)
                if digest != metadata.assetDigests?[name], let fileURL = assetFileURL(for: data) {
                    record[name] = CKAsset(fileURL: fileURL)
                    record.encryptedValues[Field.assetDigest(name)] = digest
                }
            } else if pendingColumns.contains(name) {
                // NULL because the server's asset hasn't been downloaded yet,
                // not because it was deleted here: leave the field untouched
                // (unchanged fields aren't sent), preserving the server's asset.
            } else {
                record[name] = nil
                record.encryptedValues[Field.assetDigest(name)] = nil
            }
        }
        record.encryptedValues[Field.lastModified] = metadata.syncLastModifiedDate ?? Date()
        return record
    }

    // MARK: - Fingerprints
    //
    // A fingerprint is a digest over the synced columns, the asset digests, and the last-write
    // timestamp. The row's own fingerprint equals the stored one exactly when the row has no
    // local edits. Values are canonicalized so SQLite affinity (an integer stored as a double or
    // vice versa) and sub-millisecond date noise can't produce false mismatches.

    internal static func fingerprint(values: [(name: String, value: Blackbird.Value)], assetDigests: [(name: String, digest: String?)], lastModified: Date?) -> Data {
        var hasher = SHA256()
        func feed(_ string: String) {
            let utf8 = Array(string.utf8)
            withUnsafeBytes(of: UInt32(utf8.count).littleEndian) { hasher.update(bufferPointer: $0) }
            hasher.update(data: utf8)
        }
        func feed(_ data: Data) {
            withUnsafeBytes(of: UInt32(data.count).littleEndian) { hasher.update(bufferPointer: $0) }
            hasher.update(data: data)
        }
        for (name, value) in values.sorted(by: { $0.name < $1.name }) {
            feed(name)
            switch canonical(value) {
                case .null: feed("n")
                case .integer(let i): feed("i\(i)")
                case .double(let d): feed("d\(d.bitPattern)")
                case .text(let s): feed("t"); feed(s)
                case .data(let d): feed("b"); feed(d)
            }
        }
        for (name, digest) in assetDigests.sorted(by: { $0.name < $1.name }) {
            feed("a" + name)
            feed(digest ?? "")
        }
        feed("m")
        feed(lastModified.map { String(Int64(($0.timeIntervalSince1970 * 1000).rounded())) } ?? "")
        return Data(hasher.finalize())
    }

    /// Doubles with integral values compare equal to the integers they came from.
    private static func canonical(_ value: Blackbird.Value) -> Blackbird.Value {
        if case .double(let d) = value, d.isFinite, d == d.rounded(), abs(d) < 9_007_199_254_740_992 { return .integer(Int64(d)) }
        return value
    }

    /// The row's fingerprint, as the server would see it if uploaded now.
    internal static func localFingerprint<T: BlackbirdSkybridgeSyncable>(of instance: T, metadata: SkybridgeMetadata) -> Data {
        let row = rowRepresentation(of: instance)
        let values = T.skybridgeSyncedColumnNames().map { ($0, row[$0] ?? .null) }
        let pending = Set(metadata.pendingAssetColumns ?? [])
        let digests: [(String, String?)] = T.skybridgeAssetColumnNames().map { name in
            if case .data(let data) = row[name] ?? .null { return (name, assetDigest(data)) }
            // A pending column's NULL is awaiting download, not a local edit: it still matches the server's asset.
            if pending.contains(name) { return (name, metadata.assetDigests?[name]) }
            return (name, nil)
        }
        return fingerprint(values: values, assetDigests: digests, lastModified: metadata.syncLastModifiedDate)
    }

    /// A record's fingerprint. Falls back to modificationDate like applyServerRecords does, so a
    /// record from a client that doesn't set the field can't re-upload once per receiving device.
    internal static func recordFingerprint<T: BlackbirdSkybridgeSyncable>(_ record: CKRecord, for type: T.Type) -> Data {
        let values = T.skybridgeSyncedColumnNames().map { ($0, blackbirdValue(from: record.encryptedValues[$0])) }
        let digests: [(String, String?)] = T.skybridgeAssetColumnNames().map { ($0, record.encryptedValues[Field.assetDigest($0)] as? String) }
        return fingerprint(values: values, assetDigests: digests, lastModified: (record.encryptedValues[Field.lastModified] as? Date) ?? record.modificationDate)
    }

    private static func assetDigests<T: BlackbirdSkybridgeSyncable>(from record: CKRecord, for type: T.Type) -> [String: String] {
        var digests: [String: String] = [:]
        for name in T.skybridgeAssetColumnNames() {
            if let digest = record.encryptedValues[Field.assetDigest(name)] as? String { digests[name] = digest }
        }
        return digests
    }

    private static func primaryKeyName<T: BlackbirdSkybridgeSyncable>(_ type: T.Type) -> String {
        T.skybridgePrimaryKeyInfo().name
    }

    private static func columnValue<T: BlackbirdSkybridgeSyncable>(of instance: T, at keyPath: PartialKeyPath<T>) -> Blackbird.Value {
        let raw = instance[keyPath: keyPath]
        let value = (raw as? any SkybridgeColumnReading)?.skybridgeAnyValue ?? raw
        return (try? Blackbird.Value.fromAny(value)) ?? .null
    }

    private static func primaryKeyValue<T: BlackbirdSkybridgeSyncable>(of instance: T) -> Blackbird.Value {
        guard let keyPath = T.primaryKey.first else { fatalError("Skybridge: \(T.tableName) must declare a single-column primaryKey") }
        return columnValue(of: instance, at: keyPath)
    }

    /// Every column of an instance as a raw row, keyed by column name.
    private static func rowRepresentation<T: BlackbirdSkybridgeSyncable>(of instance: T) -> Blackbird.Row {
        var row = Blackbird.Row()
        for child in Mirror(reflecting: instance).children {
            guard let label = child.label, label.hasPrefix("_"), let column = child.value as? any SkybridgeColumnReading else { continue }
            row[String(label.dropFirst())] = (try? Blackbird.Value.fromAny(column.skybridgeAnyValue)) ?? .null
        }
        return row
    }

    private static func setValue(_ value: Blackbird.Value, on record: CKRecord, forKey key: String) {
        switch value {
            case .null: record.encryptedValues[key] = nil
            case .integer(let i): record.encryptedValues[key] = NSNumber(value: i)
            case .double(let d): record.encryptedValues[key] = NSNumber(value: d)
            case .text(let s): record.encryptedValues[key] = s
            case .data(let d): record.encryptedValues[key] = d
        }
    }

    private static func blackbirdValue(from recordValue: (any CKRecordValueProtocol)?) -> Blackbird.Value {
        switch recordValue {
            case nil: return .null
            case let s as String: return .text(s)
            case let d as Data: return .data(d)
            case let date as Date: return .double(date.timeIntervalSince1970)
            case let n as NSNumber:
                let encodedType = String(cString: n.objCType)
                return (encodedType == "d" || encodedType == "f") ? .double(n.doubleValue) : .integer(n.int64Value)
            default: return .null
        }
    }

    // MARK: - Asset staging

    private static func assetDigest(_ data: Data) -> String {
        SHA256.hash(data: data).map { String(format: "%02x", $0) }.joined()
    }

    private static var assetStagingDirectory: URL {
        FileManager.default.temporaryDirectory.appendingPathComponent("BlackbirdSkybridgeAssets", isDirectory: true)
    }

    /// Stages asset contents in a temporary file for CKAsset to upload from.
    /// Content-addressed naming, so re-uploads of the same bytes reuse one
    /// file and nothing is ever staged twice.
    private static func assetFileURL(for data: Data) -> URL? {
        let directory = assetStagingDirectory
        try? FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let fileURL = directory.appendingPathComponent(assetDigest(data))
        if !FileManager.default.fileExists(atPath: fileURL.path) {
            do { try data.write(to: fileURL) } catch { return nil }
        }
        return fileURL
    }

    /// Staged files can't be deleted at any known-safe moment (the engine
    /// uploads them at its own pace), so week-old leftovers are swept at startup.
    private nonisolated static func cleanUpStagedAssets() {
        let cutoff = Date(timeIntervalSinceNow: -7 * 86400)
        guard let files = try? FileManager.default.contentsOfDirectory(at: assetStagingDirectory, includingPropertiesForKeys: [.contentModificationDateKey]) else { return }
        for fileURL in files {
            let modified = (try? fileURL.resourceValues(forKeys: [.contentModificationDateKey]))?.contentModificationDate ?? .distantPast
            if modified < cutoff {
                try? FileManager.default.removeItem(at: fileURL)
            }
        }
    }

    // MARK: - Record archives

    /// Original-format archive of a whole record. Read only to upgrade old metadata.
    private static func decodeRecord(_ data: Data) -> CKRecord? {
        try? NSKeyedUnarchiver.unarchivedObject(ofClass: CKRecord.self, from: data)
    }

    /// Current-format archive: system fields only (record ID, change tag, dates), a few hundred bytes.
    internal static func encodeSystemFields(_ record: CKRecord) -> Data {
        let archiver = NSKeyedArchiver(requiringSecureCoding: true)
        record.encodeSystemFields(with: archiver)
        archiver.finishEncoding()
        return archiver.encodedData
    }

    internal static func decodeSystemFields(_ data: Data) -> CKRecord? {
        guard let unarchiver = try? NSKeyedUnarchiver(forReadingFrom: data) else { return nil }
        unarchiver.requiresSecureCoding = true
        let record = CKRecord(coder: unarchiver)
        unarchiver.finishDecoding()
        return record
    }
}

@available(macOS 14, iOS 17, tvOS 17, watchOS 10, *)
extension BlackbirdSkybridge: CKSyncEngineDelegate {
    public func handleEvent(_ event: CKSyncEngine.Event, syncEngine: CKSyncEngine) async {
        switch event {
            case .stateUpdate(let stateUpdate):
                await persistState(stateUpdate.stateSerialization)
            case .accountChange(let accountChange):
                await handleAccountChange(accountChange)
            case .fetchedDatabaseChanges(let changes):
                await handleFetchedDatabaseChanges(changes)
            case .fetchedRecordZoneChanges(let changes):
                await handleFetchedRecordZoneChanges(changes)
            case .sentRecordZoneChanges(let sent):
                await handleSentRecordZoneChanges(sent)
            case .didSendChanges:
                await topUpPendingChanges()
            case .sentDatabaseChanges, .willFetchChanges, .willFetchRecordZoneChanges, .didFetchRecordZoneChanges, .didFetchChanges, .willSendChanges:
                break
            @unknown default:
                break
        }
    }

    public func nextRecordZoneChangeBatch(_ context: CKSyncEngine.SendChangesContext, syncEngine: CKSyncEngine) async -> CKSyncEngine.RecordZoneChangeBatch? {
        let scope = context.options.scope
        let pendingChanges = syncEngine.state.pendingRecordZoneChanges.filter { scope.contains($0) }
        guard !pendingChanges.isEmpty else { return nil }

        return await CKSyncEngine.RecordZoneChangeBatch(pendingChanges: pendingChanges) { recordID in
            guard let (model, primaryKey) = await self.modelAndPrimaryKey(for: recordID),
                  let record = await self.recordForUpload(model, primaryKey: primaryKey, recordID: recordID)
            else {
                // Unknown table, or deleted locally since it was queued.
                syncEngine.state.remove(pendingRecordZoneChanges: [.saveRecord(recordID)])
                return nil
            }
            return record
        }
    }
}

private extension Array {
    func chunked(into size: Int) -> [[Element]] {
        guard size > 0, !isEmpty else { return isEmpty ? [] : [self] }
        return stride(from: 0, to: count, by: size).map { Array(self[$0 ..< Swift.min($0 + size, count)]) }
    }
}

#endif // canImport(CloudKit)
