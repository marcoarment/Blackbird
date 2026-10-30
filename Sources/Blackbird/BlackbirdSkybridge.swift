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
/// Synced columns move as raw SQL values, never through the model's decoding, so
/// a value this build doesn't understand (an enum case added in a newer one, say)
/// is stored and sent back exactly as it arrived rather than as what the model
/// makes of it.
///
/// Rows sync in CloudKit record zones. By default every row of every type goes
/// in one private zone. A type can instead name a ``skybridgeZoneColumn`` whose
/// value picks each row's zone (a document ID, say), which is what makes a
/// document shareable: CloudKit shares whole zones, so everything in a document
/// must be in a zone of its own, and a row can never move between zones. Zones
/// are created as rows for them first appear, and zones shared with the user by
/// other people are synced from the shared database alongside the user's own.
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

    /// The column whose value says which zone each row syncs in, e.g. `\.$documentID`;
    /// an integer or text column, and one that never changes for a row. Default: nil,
    /// every row of the type in the default zone.
    static var skybridgeZoneColumn: BlackbirdColumnKeyPath? { get }

    /// The zone key for a value of the zone column: the zone's name, which must be the value's
    /// own text form (the same as SQLite's `CAST(value AS TEXT)`), or nil for the default zone.
    /// Default: nil for NULL, otherwise the value as text. Override to send some values' rows to
    /// the default zone instead, e.g. an ID that stands for "no document".
    static func skybridgeZoneKey(for zoneColumnValue: Blackbird.Value) -> String?

    /// Whether a record deleted on the server takes the local row with it even when the row was
    /// edited here meanwhile. Default: false, the local edit wins and recreates the record. True
    /// suits rows that are deleted outright rather than tombstoned, where a deletion is as
    /// deliberate as an edit and the edit would otherwise bring back what someone removed.
    static var skybridgeServerDeletionWins: Bool { get }

    /// Resolves a conflict between a row edited here and the server's version of it, written
    /// elsewhere since: returns the synced column values to keep, or nil to keep whichever was
    /// written last (the default). Both rows hold the synced columns only. A result that differs
    /// from the server's values is uploaded as a new edit; the local row is never left as it was
    /// unless the result says so.
    static func skybridgeMerge(local: Blackbird.Row, server: Blackbird.Row, localIsNewer: Bool) -> Blackbird.Row?
}

extension BlackbirdSkybridgeSyncable {
    public static var skybridgeExcludedColumns: [BlackbirdColumnKeyPath] { [] }
    public static var skybridgeAssetColumns: [BlackbirdColumnKeyPath] { [] }
    public static var skybridgeZoneColumn: BlackbirdColumnKeyPath? { nil }
    public static func skybridgeZoneKey(for zoneColumnValue: Blackbird.Value) -> String? {
        if case .null = zoneColumnValue { return nil }
        return zoneColumnValue.stringValue
    }
    public static var skybridgeServerDeletionWins: Bool { false }
    public static func skybridgeMerge(local: Blackbird.Row, server: Blackbird.Row, localIsNewer: Bool) -> Blackbird.Row? { nil }
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
    var hasServerBase: Bool { syncedFingerprint != nil || ckRecordData != nil || systemFields != nil || packedSystemFields != nil }

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

    internal static func skybridgeZoneColumnName() -> String? {
        skybridgeZoneColumn.map { columnInfoFromKeyPath($0).name }
    }

    /// The zone key of a row, from its zone column.
    internal static func skybridgeZoneKey(of row: Blackbird.Row) -> String? {
        guard let column = skybridgeZoneColumnName() else { return nil }
        return skybridgeZoneKey(for: row[column] ?? .null)
    }

    /// The zone column's SQL type, if the type has one.
    internal static func skybridgeZoneColumnType() -> Blackbird.ColumnType? {
        guard let name = skybridgeZoneColumnName() else { return nil }
        return table.columns.first { $0.name == name }?.columnType
    }

    /// A zone column value from its text form, as a zone key or a trigger's `CAST(... AS TEXT)` gives it.
    internal static func skybridgeZoneColumnValue(fromText text: String?) -> Blackbird.Value {
        guard let text, let type = skybridgeZoneColumnType() else { return .null }
        switch type {
            case .integer: return Int64(text).map { .integer($0) } ?? .text(text)
            case .double: return Double(text).map { .double($0) } ?? .text(text)
            case .text, .data: return .text(text)
        }
    }

    /// The zone key a trigger's recorded zone value stands for.
    internal static func skybridgeZoneKey(fromText text: String?) -> String? {
        skybridgeZoneKey(for: skybridgeZoneColumnValue(fromText: text))
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
        if let type = skybridgeZoneColumnType(), type == .data || type == .double {
            fatalError("Skybridge: \(tableName).\(skybridgeZoneColumnName() ?? "?") is declared as the zone column but isn't an integer or text column")
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
/// One row per engine: the private database's and the shared database's.
internal struct BlackbirdSkybridgeState: BlackbirdModel {
    @BlackbirdColumn var id: Int
    @BlackbirdColumn var stateSerialization: Data?

    static let singletonID = 0
    static let privateEngineID = 0
    static let sharedEngineID = 1
}

/// Housekeeping that survives launches: when the last full reconcile ran, whether every row's
/// metadata has been upgraded to the current format, and which zone layout the records follow.
internal struct BlackbirdSkybridgeInfo: BlackbirdModel {
    @BlackbirdColumn var id: Int
    @BlackbirdColumn var lastFullReconcile: Date?
    @BlackbirdColumn var metadataUpgraded: Bool
    /// 0: every row in the default zone, as before zone columns existed. 1: rows in their zones.
    @BlackbirdColumn var zoneLayoutVersion: Int
    /// The CloudKit environment the sync state belongs to, as the app names it; nil before
    /// environments were tracked.
    @BlackbirdColumn var environment: String?

    static let singletonID = 0
    static let currentZoneLayoutVersion = 1
}

/// A flag the deletion triggers consult: set while Skybridge itself writes synced tables, so
/// its own writes (applying server records, bookkeeping, removing a zone's rows) aren't marked
/// as local edits.
internal struct BlackbirdSkybridgeFlags: BlackbirdModel {
    @BlackbirdColumn var id: Int
    @BlackbirdColumn var suppressTriggers: Int

    static let singletonID = 0
}

/// Rows written or deleted locally that haven't been reconciled with the server yet, kept by
/// triggers on every synced table so nothing depends on the app staying alive between a write
/// and its reconcile. Startup reconciles only these instead of every row of every table. A
/// deletion's marker carries the row's zone column as text, since the row itself is gone.
internal struct BlackbirdSkybridgeDirtyRow: BlackbirdModel {
    static let primaryKey: [BlackbirdColumnKeyPath] = [ \.$recordName ]

    @BlackbirdColumn var recordName: String
    @BlackbirdColumn var tableName: String
    @BlackbirdColumn var primaryKeyString: String
    @BlackbirdColumn var isDeletion: Bool
    @BlackbirdColumn var zoneValue: String?

    var id: String { recordName }
}

/// Record changes waiting to be handed to a sync engine. The engine's own pending list is
/// persisted whole on every state update, so bulk operations (first upload, purges) park their
/// changes here and feed the engine a slice at a time.
internal struct BlackbirdSkybridgeQueuedChange: BlackbirdModel {
    static let primaryKey: [BlackbirdColumnKeyPath] = [ \.$recordName ]

    @BlackbirdColumn var recordName: String
    @BlackbirdColumn var isDeletion: Bool
    @BlackbirdColumn var zoneName: String
    @BlackbirdColumn var ownerName: String

    var id: String { recordName }
}

/// Every zone rows sync in other than the default one: the user's own, and those shared with
/// them. A removed zone (a share revoked or left, a zone purged elsewhere) stays as a tombstone
/// for a while, so a row written for it by a stale editor is dropped rather than uploaded into
/// a brand-new zone of the same name.
internal struct BlackbirdSkybridgeZone: BlackbirdModel {
    static let primaryKey: [BlackbirdColumnKeyPath] = [ \.$key ]

    @BlackbirdColumn var key: String
    /// The CloudKit user that owns the zone; nil for the user's own, in the private database.
    @BlackbirdColumn var ownerName: String?
    /// The zone's `CKShare`, archived, once it has one.
    @BlackbirdColumn var share: Data?
    /// `CKShare.ParticipantPermission` of the user in the zone.
    @BlackbirdColumn var permission: Int
    @BlackbirdColumn var added: Date
    @BlackbirdColumn var removedDate: Date?

    var id: String { key }
    var isOwn: Bool { ownerName == nil }
    var zoneID: CKRecordZone.ID { CKRecordZone.ID(zoneName: key, ownerName: ownerName ?? CKCurrentUserDefaultName) }
}

/// Syncs ``BlackbirdSkybridgeSyncable`` tables to CloudKit via CKSyncEngine.
///
/// Sync is fully automatic and silent: conflicts resolve to the latest write (or as the type's
/// ``BlackbirdSkybridgeSyncable/skybridgeMerge(local:server:localIsNewer:)`` says), and errors
/// are logged and retried without ever involving the user.
///
/// Each row's metadata holds a fingerprint of its content as last confirmed with the server.
/// Comparing a row's own fingerprint against it distinguishes genuine local edits (which get
/// uploaded) from echoes of sync-applied writes (ignored), so no in-memory bookkeeping is
/// needed and devices can't ping-pong updates.
///
/// Rows sync as records with the record type set to the table name and the record name set to
/// "tableName:primaryKey", in the private "Skybridge" zone unless their type has a
/// ``BlackbirdSkybridgeSyncable/skybridgeZoneColumn``, in which case each row goes in the zone
/// its column names. Zones belonging to other users, shared with this one, sync from the shared
/// database; the user's own, shared or not, from the private one. What happens to zones (one
/// shared with the user arrives, a share changes, a zone is revoked or deleted elsewhere) is
/// reported through ``setZoneEventHandler(_:)``, for the app to keep whatever it holds about
/// them in step.
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

    /// How many changes an engine holds at once before the rest wait in the queue table.
    private static let engineSliceLimit = 2000
    /// Rows per transaction for bulk work.
    private static let batchSize = 400
    /// How often the paginated full reconcile runs.
    private static let fullReconcileInterval: TimeInterval = 24 * 3600
    /// How long a removed zone is remembered.
    private static let removedZoneRetention: TimeInterval = 7 * 24 * 3600

    /// The private zone rows sync in when their type has no zone column, or their zone key is nil.
    public static let defaultZoneName = "Skybridge"
    private static let defaultZoneID = CKRecordZone.ID(zoneName: defaultZoneName, ownerName: CKCurrentUserDefaultName)

    // MARK: - Zones, as the app sees them

    /// A zone other than the default one, as known here.
    public struct Zone: Sendable, Hashable {
        public enum Permission: Sendable, Hashable { case readOnly, readWrite }

        public struct Participant: Sendable, Hashable {
            /// The participant's CloudKit user record name, the same for them in every share;
            /// nil for a participant CloudKit hasn't resolved to a user yet.
            public let userRecordName: String?
            /// The participant's name as CloudKit knows it, if it does.
            public let name: String?
            public let isCurrentUser: Bool
            public let isOwner: Bool
            public let permission: Permission
            public let hasAccepted: Bool
        }

        public let key: String
        /// The CloudKit user the zone belongs to; nil for the user's own.
        public let ownerName: String?
        public var isOwn: Bool { ownerName == nil }
        /// Whether the zone is shared: the user's own with a share, or someone else's shared in.
        public let isShared: Bool
        /// What the user may do in the zone. Writes a read-only participant makes are refused by
        /// the server and undone here, so the app should keep them from being made.
        public let permission: Permission
        /// The share's participants, the owner among them; empty when the zone isn't shared.
        public let participants: [Participant]
        /// The zone's `CKShare`, archived, for the sharing interface to present.
        public let shareData: Data?

        /// The zone's share, if it has one.
        public func share() -> CKShare? { shareData.flatMap { BlackbirdSkybridge.decodeShare($0) } }
    }

    /// Something that happened to a zone, reported to the app.
    public enum ZoneEvent: Sendable {
        /// A zone appeared that this device hadn't seen: one of the user's own from another
        /// device, or one shared with the user (`zone.isOwn` says which).
        case added(Zone)
        /// The zone's share changed: it was shared or unshared, or its participants or
        /// permissions changed.
        case shareChanged(Zone)
        /// The zone is gone, and its rows with it.
        case removed(key: String, reason: RemovalReason)
    }

    public enum RemovalReason: Sendable {
        /// The user's own zone was deleted from another device (see ``removeZone(key:)``).
        case deletedElsewhere
        /// The owner stopped sharing the zone with the user, or deleted it.
        case shareRevoked
        /// The user left the share (see ``leaveShare(zoneKey:)``).
        case left
        /// The user signed out of iCloud or switched accounts; the zone belonged to the old one.
        case accountChanged
        /// The user removed the zone (see ``removeZone(key:)``).
        case removed
        /// The app now syncs with another CloudKit environment, where the zone isn't shared
        /// with the user (see ``init(syncing:environment:unrecordedEnvironment:)``).
        case environmentChanged
    }

    private let syncedTypes: [any BlackbirdSkybridgeSyncable.Type]
    private let environment: String?
    private let unrecordedEnvironment: String?

    private var models: [String: any BlackbirdSkybridgeSyncable.Type] = [:] // keyed by tableName == CKRecord type
    private var database: Blackbird.Database?
    private var privateEngine: CKSyncEngine?
    private var sharedEngine: CKSyncEngine?
    /// The zone registry, as in its table.
    private var zones: [String: BlackbirdSkybridgeZone] = [:]
    private var zoneEventHandler: (@Sendable (ZoneEvent) async -> Void)?
    private var fetchActivityHandler: (@Sendable (FetchActivity) async -> Void)?
    /// The engines mid-fetch, by ID, and the zones mid-fetch: the engine's own fetches report
    /// zone by zone, without the enclosing fetch events a requested fetch gets.
    private var fetchingEngines: Set<Int> = []
    private var fetchingZones: Set<String> = []
    private var localChangeTasks: [Task<Void, Never>] = []
    private var toppingUp: Set<Int> = []
    /// When each engine last finished sending a batch, for the send watchdog.
    private var lastSendProgress: [Int: Date] = [:]
    /// Records whose deletion has been handed to an engine to send and not yet answered, by
    /// name. A fetched copy of such a record (the echo of its earlier save, say) must not put
    /// the row back meanwhile.
    private var inFlightDeletions: Set<String> = []
    /// Records the server confirmed deleting from here lately, by name, with when: a copy of
    /// one fetched after that, from before the deletion, is stale, unless the row was written
    /// here again since.
    private var recentDeletions: [String: Date] = [:]
    private static let recentDeletionWindow: TimeInterval = 120
    private var sendWatchdog: Task<Void, Never>?
    /// How long an engine may hold pending changes without sending before it's asked to send.
    private static let sendStallInterval: TimeInterval = 60

    /// Whether ``start(database:)`` found the local sync state belonging to another CloudKit
    /// environment, and started over in this one: everything is uploaded again, and anything the
    /// app keeps in CloudKit outside sync needs saving again too.
    public private(set) var startedInNewEnvironment = false

    /// - Parameters:
    ///   - environment: The CloudKit environment the app syncs with, in any form the app likes
    ///     (e.g. "Development" or "Production"). When it differs from the one the local state last
    ///     synced with, sync starts over in it: every row of the user's own zones is uploaded
    ///     afresh, and zones shared with the user in the other environment go. Nil not to track it.
    ///   - unrecordedEnvironment: The environment to take the local state as belonging to when
    ///     none is recorded, as by versions before environments were tracked; nil to take it as
    ///     belonging to `environment`.
    public init(syncing syncedTypes: [any BlackbirdSkybridgeSyncable.Type], environment: String? = nil, unrecordedEnvironment: String? = nil) {
        self.syncedTypes = syncedTypes
        self.environment = environment
        self.unrecordedEnvironment = unrecordedEnvironment
    }

    /// Receives zone events. Set before ``start(database:)`` to miss none.
    public func setZoneEventHandler(_ handler: (@Sendable (ZoneEvent) async -> Void)?) {
        zoneEventHandler = handler
    }

    /// What fetching from the server is up to, for the app to show that more is on its way.
    public enum FetchActivity: Sendable {
        /// A fetch began; nothing may arrive for a while (the server can hold a fetch back).
        case started
        /// A batch of records was fetched and applied here.
        case applied(records: Int)
        /// No fetch is in progress any more.
        case finished
    }

    /// Receives what fetching is up to: `started` and `finished` as whether anything is being
    /// fetched changes, and `applied` as each batch lands. Set before ``start(database:)`` to
    /// miss none.
    public func setFetchActivityHandler(_ handler: (@Sendable (FetchActivity) async -> Void)?) {
        fetchActivityHandler = handler
    }

    /// Whether anything is being fetched from the server right now.
    public var isFetching: Bool { !fetchingEngines.isEmpty || !fetchingZones.isEmpty }

    private func noteFetching(_ fetching: Bool, engine: CKSyncEngine, zoneID: CKRecordZone.ID? = nil) async {
        let before = isFetching
        if let zoneID {
            let key = "\(zoneID.ownerName)/\(zoneID.zoneName)"
            if fetching { fetchingZones.insert(key) } else { fetchingZones.remove(key) }
        } else {
            let id = engine === privateEngine ? BlackbirdSkybridgeState.privateEngineID : BlackbirdSkybridgeState.sharedEngineID
            if fetching { fetchingEngines.insert(id) } else { fetchingEngines.remove(id) }
        }
        if isFetching != before { await fetchActivityHandler?(isFetching ? .started : .finished) }
    }

    /// Begins syncing. The database must already have resolved the schemas of every synced type.
    public func start(database: Blackbird.Database) async {
        guard self.database == nil else { return }
        self.database = database

        _ = try? await BlackbirdSkybridgeState.resolveSchema(in: database)
        _ = try? await BlackbirdSkybridgeInfo.resolveSchema(in: database)
        _ = try? await BlackbirdSkybridgeFlags.resolveSchema(in: database)
        _ = try? await BlackbirdSkybridgeDirtyRow.resolveSchema(in: database)
        _ = try? await BlackbirdSkybridgeQueuedChange.resolveSchema(in: database)
        _ = try? await BlackbirdSkybridgeZone.resolveSchema(in: database)

        for type in syncedTypes {
            type.skybridgeValidate()
            models[type.tableName] = type
        }

        // The flag row the triggers consult, and the triggers themselves, made afresh: a table
        // rebuilt by a schema change loses its triggers.
        try? await database.transaction { core in
            try core.query("INSERT OR IGNORE INTO `\(BlackbirdSkybridgeFlags.tableName)` (id, suppressTriggers) VALUES (?, 0)", BlackbirdSkybridgeFlags.singletonID)
            for type in self.syncedTypes { try Self.installTriggers(type, in: core) }
        }

        for zone in (try? await BlackbirdSkybridgeZone.read(from: database, matching: .all)) ?? [] { zones[zone.key] = zone }
        await forgetOldRemovedZones()
        await startOverIfEnvironmentChanged()

        let privateState = await savedState(id: BlackbirdSkybridgeState.privateEngineID)
        let sharedState = await savedState(id: BlackbirdSkybridgeState.sharedEngineID)
        let container = CKContainer.default()
        privateEngine = CKSyncEngine(CKSyncEngine.Configuration(database: container.privateCloudDatabase, stateSerialization: privateState, delegate: self))
        sharedEngine = CKSyncEngine(CKSyncEngine.Configuration(database: container.sharedCloudDatabase, stateSerialization: sharedState, delegate: self))
        Self.logger.info("☁️ Skybridge started: \(self.models.keys.sorted().joined(separator: ", "), privacy: .public); \(self.zones.count) zones")

        Task.detached(priority: .background) { Self.cleanUpStagedAssets() }

        if privateState == nil {
            // First run (or state was lost, or belonged to another environment): create the zones
            // and upload everything, each row to its zone, so there's nothing to move into zones later.
            var info = await currentInfo()
            info.zoneLayoutVersion = BlackbirdSkybridgeInfo.currentZoneLayoutVersion
            try? await info.write(to: database)
            await scheduleFullUpload(clearingServerRecords: false)
        } else {
            await migrateZoneLayoutIfNeeded()
        }

        for type in syncedTypes {
            let tableName = type.tableName
            localChangeTasks.append(Task { [weak self] in
                for await primaryKeys in type.skybridgeChangeStream(in: database) {
                    await self?.handleLocalChange(tableName: tableName, primaryKeys: primaryKeys)
                }
            })
        }

        // Rows written or deleted since their last reconcile (e.g. the app was killed mid-sync).
        await reconcileDirtyRows(tableName: nil)
        await topUpPendingChanges()

        Task(priority: .background) { [weak self] in
            await self?.runHousekeeping()
        }
        startSendWatchdog()
    }

    /// Forgets everything that belongs to the environment the local state last synced with, when
    /// that isn't this one: the engines' state, so they fetch everything afresh; changes queued
    /// for its records; zones shared with the user there, with their rows; the shares of the
    /// user's own zones; and every row's server base. The rows of the user's own zones stay, to be
    /// uploaded afresh.
    ///
    /// The server bases go here, before the engines exist, rather than as the full upload reaches
    /// them: a new engine reports a sign-in, whose own full upload doesn't clear them, and a row
    /// sent on a base from the other environment is a record this one doesn't have. Rows of types
    /// whose server deletion wins would be deleted for it.
    private func startOverIfEnvironmentChanged() async {
        guard let database, let environment else { return }
        var info = await currentInfo()
        let previous = info.environment ?? unrecordedEnvironment
        if info.environment != environment {
            info.environment = environment
            try? await info.write(to: database)
        }
        guard let previous, previous != environment, await savedState(id: BlackbirdSkybridgeState.privateEngineID) != nil else { return }

        Self.logger.info("☁️ Skybridge: local state belongs to the \(previous, privacy: .public) environment; starting over in \(environment, privacy: .public)")
        startedInNewEnvironment = true
        _ = try? await database.query("DELETE FROM `\(BlackbirdSkybridgeState.tableName)`")
        _ = try? await database.query("DELETE FROM `\(BlackbirdSkybridgeQueuedChange.tableName)`")
        for zone in zones.values {
            if zone.removedDate != nil {
                zones.removeValue(forKey: zone.key)
                try? await zone.delete(from: database)
            } else if !zone.isOwn {
                await removeLocalZone(key: zone.key, reason: .environmentChanged)
                if let removed = zones.removeValue(forKey: zone.key) { try? await removed.delete(from: database) }
            } else if zone.share != nil {
                var unshared = zone
                unshared.share = nil
                unshared.permission = CKShare.ParticipantPermission.readWrite.rawValue
                await write(zone: unshared)
                await zoneEventHandler?(.shareChanged(publicZone(unshared)))
            }
        }
        for model in models.values { await clearServerBases(model) }
    }

    /// Discards the server base of every row of a table, in batches.
    private func clearServerBases<T: BlackbirdSkybridgeSyncable>(_ type: T.Type) async {
        guard let database else { return }
        var lastRowID: Int64 = 0
        var clearedCount = 0
        while true {
            let rows = (try? await T.query(in: database, "SELECT rowid AS __rowid, `\(Self.primaryKeyName(T.self))` AS __pk, `\(skybridgeMetadataColumnName)` AS __meta FROM `\(T.tableName)` WHERE rowid > ? AND `\(skybridgeMetadataColumnName)` IS NOT NULL ORDER BY rowid LIMIT \(Self.batchSize)", lastRowID)) ?? []
            guard !rows.isEmpty else { break }
            var cleared: [(key: Blackbird.Value, metadata: SkybridgeMetadata)] = []
            for row in rows {
                guard let rowID = row["__rowid"]?.int64Value, let key = row["__pk"] else { continue }
                lastRowID = max(lastRowID, rowID)
                guard case .data(let data) = row["__meta"] ?? .null else { continue }
                var metadata = SkybridgeMetadata.decode(data)
                metadata.clearServerBase()
                cleared.append((key, metadata))
            }
            let batch = cleared
            try? await database.transaction { core in
                try Self.suppressingTriggers(core) {
                    for (key, metadata) in batch { try Self.writeMetadata(metadata, for: T.self, primaryKey: key, in: core) }
                }
            }
            clearedCount += batch.count
            await Task.yield()
        }
        Self.logger.info("☁️ Skybridge cleared the server bases of \(clearedCount) \(T.tableName, privacy: .public) rows")
    }

    private func savedState(id: Int) async -> CKSyncEngine.State.Serialization? {
        guard let database, let saved = try? await BlackbirdSkybridgeState.read(from: database, id: id), let data = saved.stateSerialization else { return nil }
        return try? JSONDecoder().decode(CKSyncEngine.State.Serialization.self, from: data)
    }

    /// Slow, resumable work that never blocks the app: upgrading metadata left in the original
    /// format, retrying pending assets, and the periodic full reconcile.
    private func runHousekeeping() async {
        guard let database else { return }
        let info = await currentInfo()

        if !info.metadataUpgraded {
            for model in models.values { await upgradeMetadata(model) }
            var upgraded = await currentInfo()
            upgraded.metadataUpgraded = true
            try? await upgraded.write(to: database)
            Self.logger.info("☁️ Skybridge metadata upgraded to the current format")
        }

        await refetchRecordsWithPendingAssets()

        if (info.lastFullReconcile ?? .distantPast) < Date(timeIntervalSinceNow: -Self.fullReconcileInterval) {
            for model in models.values { await reconcileAllRows(model) }
            var done = await currentInfo()
            done.lastFullReconcile = Date()
            try? await done.write(to: database)
        }
    }

    private func currentInfo() async -> BlackbirdSkybridgeInfo {
        guard let database, let info = try? await BlackbirdSkybridgeInfo.read(from: database, id: BlackbirdSkybridgeInfo.singletonID) else {
            return BlackbirdSkybridgeInfo(id: BlackbirdSkybridgeInfo.singletonID, lastFullReconcile: nil, metadataUpgraded: false, zoneLayoutVersion: 0, environment: nil)
        }
        return info
    }

    /// Moves rows that synced before zone columns existed out of the default zone into their
    /// own: each is uploaded afresh into its zone, and its old record deleted. Other devices
    /// ignore the old records' deletions, since the rows no longer live in that zone there either.
    private func migrateZoneLayoutIfNeeded() async {
        guard let database else { return }
        var info = await currentInfo()
        guard info.zoneLayoutVersion < BlackbirdSkybridgeInfo.currentZoneLayoutVersion else { return }
        Self.logger.info("☁️ Skybridge moving records into their zones")
        for model in models.values where model.skybridgeZoneColumnName() != nil {
            await moveRowsIntoZones(model)
        }
        info.zoneLayoutVersion = BlackbirdSkybridgeInfo.currentZoneLayoutVersion
        try? await info.write(to: database)
        await topUpPendingChanges()
    }

    private func moveRowsIntoZones<T: BlackbirdSkybridgeSyncable>(_ type: T.Type) async {
        guard let database, let zoneColumn = T.skybridgeZoneColumnName() else { return }
        var lastRowID: Int64 = 0
        while true {
            let rows = (try? await T.query(in: database, "SELECT rowid AS __rowid, `\(Self.primaryKeyName(T.self))` AS __pk, `\(zoneColumn)` AS __zone, `\(skybridgeMetadataColumnName)` AS __meta FROM `\(T.tableName)` WHERE rowid > ? ORDER BY rowid LIMIT \(Self.batchSize)", lastRowID)) ?? []
            guard !rows.isEmpty else { return }
            var moved: [(key: Blackbird.Value, metadata: SkybridgeMetadata, zoneKey: String)] = []
            for row in rows {
                guard let rowID = row["__rowid"]?.int64Value, let key = row["__pk"] else { continue }
                lastRowID = max(lastRowID, rowID)
                guard let zoneKey = T.skybridgeZoneKey(for: row["__zone"] ?? .null),
                      case .data(let data) = row["__meta"] ?? .null else { continue }
                var metadata = SkybridgeMetadata.decode(data)
                guard metadata.hasServerBase else { continue }
                metadata.clearServerBase()
                moved.append((key, metadata, zoneKey))
            }
            guard !moved.isEmpty else { await Task.yield(); continue }
            let batch = moved
            try? await database.transaction { core in
                try Self.suppressingTriggers(core) {
                    for (key, metadata, _) in batch { try Self.writeMetadata(metadata, for: T.self, primaryKey: key, in: core) }
                }
            }
            var changes: [(CKSyncEngine.PendingRecordZoneChange, CKRecordZone.ID)] = []
            for (key, _, zoneKey) in batch {
                guard let keyString = key.stringValue, let zoneID = await zoneIDForUpload(key: zoneKey) else { continue }
                let name = recordName(tableName: T.tableName, primaryKeyString: keyString)
                changes.append((.saveRecord(CKRecord.ID(recordName: name, zoneID: zoneID)), zoneID))
                changes.append((.deleteRecord(CKRecord.ID(recordName: name, zoneID: Self.defaultZoneID)), Self.defaultZoneID))
            }
            await enqueue(changes.map(\.0))
            await Task.yield()
        }
    }

    // MARK: - Triggers
    //
    // Every synced table gets insert, update, and delete triggers that mark the row dirty, so a
    // local change is recorded in the same transaction that makes it. A deletion's marker keeps
    // the row's zone column as text: the row is gone, and its record can't be deleted from the
    // server without knowing which zone it was in. Skybridge's own writes set a flag that the
    // triggers check, so they leave no markers.

    private static func triggerNames(_ tableName: String) -> [String] {
        ["Insert", "Update", "Delete"].map { "\(tableName)+Skybridge\($0)" }
    }

    internal static func installTriggers<T: BlackbirdSkybridgeSyncable>(_ type: T.Type, in core: isolated Blackbird.Database.Core) throws {
        let table = T.tableName
        let pk = primaryKeyName(T.self)
        let zoneColumn = T.skybridgeZoneColumnName()
        let when = "WHEN (SELECT `suppressTriggers` FROM `\(BlackbirdSkybridgeFlags.tableName)` WHERE id = \(BlackbirdSkybridgeFlags.singletonID)) IS NOT 1"
        // An upsert, not INSERT OR REPLACE: a trigger's OR REPLACE gives way to the conflict policy
        // of the statement that fired it, and Blackbird's writes are upserts that abort on any other
        // conflict, so a row written again before its marker was taken up would fail to write.
        func body(_ ref: String, isDeletion: Int) -> String {
            let zone = zoneColumn.map { "CAST(\(ref).`\($0)` AS TEXT)" } ?? "NULL"
            return "INSERT INTO `\(BlackbirdSkybridgeDirtyRow.tableName)` (`recordName`, `tableName`, `primaryKeyString`, `isDeletion`, `zoneValue`) " +
                "VALUES ('\(table):' || CAST(\(ref).`\(pk)` AS TEXT), '\(table)', CAST(\(ref).`\(pk)` AS TEXT), \(isDeletion), \(zone)) " +
                "ON CONFLICT (`recordName`) DO UPDATE SET `tableName` = excluded.`tableName`, `primaryKeyString` = excluded.`primaryKeyString`, " +
                "`isDeletion` = excluded.`isDeletion`, `zoneValue` = excluded.`zoneValue`;"
        }
        let definitions: [(name: String, event: String, body: String)] = [
            (triggerNames(table)[0], "INSERT", body("NEW", isDeletion: 0)),
            (triggerNames(table)[1], "UPDATE", body("NEW", isDeletion: 0)),
            (triggerNames(table)[2], "DELETE", body("OLD", isDeletion: 1)),
        ]
        for definition in definitions {
            try core.query("DROP TRIGGER IF EXISTS `\(definition.name)`")
            try core.query("CREATE TRIGGER `\(definition.name)` AFTER \(definition.event) ON `\(table)` \(when) BEGIN \(definition.body) END")
        }
    }

    /// Runs writes that the triggers leave unmarked: Skybridge's own.
    internal static func suppressingTriggers<R>(_ core: isolated Blackbird.Database.Core, _ body: () throws -> R) throws -> R {
        try core.query("UPDATE `\(BlackbirdSkybridgeFlags.tableName)` SET suppressTriggers = 1 WHERE id = \(BlackbirdSkybridgeFlags.singletonID)")
        defer { _ = try? core.query("UPDATE `\(BlackbirdSkybridgeFlags.tableName)` SET suppressTriggers = 0 WHERE id = \(BlackbirdSkybridgeFlags.singletonID)") }
        return try body()
    }

    // MARK: - Record identity

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

    // MARK: - Zone registry

    private static func isOwn(_ zoneID: CKRecordZone.ID) -> Bool { zoneID.ownerName == CKCurrentUserDefaultName }

    /// The zone key a zone ID stands for: nil for the user's own default zone.
    private func zoneKey(for zoneID: CKRecordZone.ID) -> String? {
        zoneID.zoneName == Self.defaultZoneName && Self.isOwn(zoneID) ? nil : zoneID.zoneName
    }

    private func engine(for zoneID: CKRecordZone.ID) -> CKSyncEngine? {
        Self.isOwn(zoneID) ? privateEngine : sharedEngine
    }

    private func engineID(for zoneID: CKRecordZone.ID) -> Int {
        Self.isOwn(zoneID) ? BlackbirdSkybridgeState.privateEngineID : BlackbirdSkybridgeState.sharedEngineID
    }

    private func cloudDatabase(for zoneID: CKRecordZone.ID) -> CKDatabase {
        Self.isOwn(zoneID) ? CKContainer.default().privateCloudDatabase : CKContainer.default().sharedCloudDatabase
    }

    /// The zone a local row uploads to: the default zone, or the row's own, registered and
    /// created if it's new here. Nil for a zone that's been removed: the row is a straggler.
    private func zoneIDForUpload(key: String?) async -> CKRecordZone.ID? {
        guard let key else { return Self.defaultZoneID }
        if let zone = zones[key] {
            return zone.removedDate == nil ? zone.zoneID : nil
        }
        let zone = await register(key: key, ownerName: nil)
        privateEngine?.state.add(pendingDatabaseChanges: [.saveZone(CKRecordZone(zoneID: zone.zoneID))])
        return zone.zoneID
    }

    /// The zone a fetched record's zone ID names, registered if it's new here. Nil for the
    /// default zone, and for zones that can't be synced (someone else's default zone).
    private func registeredZone(for zoneID: CKRecordZone.ID) async -> BlackbirdSkybridgeZone? {
        guard let key = zoneKey(for: zoneID) else { return nil }
        if zoneID.zoneName == Self.defaultZoneName { return nil }
        if let zone = zones[key] {
            if zone.removedDate != nil {
                // Back from the dead: a share accepted again, say.
                var revived = zone
                revived.removedDate = nil
                revived.ownerName = Self.isOwn(zoneID) ? nil : zoneID.ownerName
                revived.added = Date()
                await write(zone: revived)
                await zoneEventHandler?(.added(publicZone(revived)))
                return revived
            }
            return zone
        }
        let zone = await register(key: key, ownerName: Self.isOwn(zoneID) ? nil : zoneID.ownerName)
        await zoneEventHandler?(.added(publicZone(zone)))
        return zone
    }

    @discardableResult
    private func register(key: String, ownerName: String?) async -> BlackbirdSkybridgeZone {
        let zone = BlackbirdSkybridgeZone(key: key, ownerName: ownerName, share: nil, permission: CKShare.ParticipantPermission.readWrite.rawValue, added: Date(), removedDate: nil)
        await write(zone: zone)
        return zone
    }

    private func write(zone: BlackbirdSkybridgeZone) async {
        zones[zone.key] = zone
        if let database { try? await zone.write(to: database) }
    }

    private func forgetOldRemovedZones() async {
        guard let database else { return }
        let cutoff = Date(timeIntervalSinceNow: -Self.removedZoneRetention)
        for zone in zones.values {
            guard let removed = zone.removedDate, removed < cutoff else { continue }
            zones.removeValue(forKey: zone.key)
            try? await zone.delete(from: database)
        }
    }

    private func publicZone(_ zone: BlackbirdSkybridgeZone) -> Zone {
        let share = zone.share.flatMap { Self.decodeShare($0) }
        let participants: [Zone.Participant] = share?.participants.map { participant in
            Zone.Participant(
                userRecordName: participant.userIdentity.userRecordID?.recordName,
                name: participant.userIdentity.nameComponents.map { PersonNameComponentsFormatter.localizedString(from: $0, style: .default) },
                isCurrentUser: participant == share?.currentUserParticipant,
                isOwner: participant.role == .owner,
                permission: participant.permission == .readOnly ? .readOnly : .readWrite,
                hasAccepted: participant.acceptanceStatus == .accepted
            )
        } ?? []
        return Zone(key: zone.key, ownerName: zone.ownerName, isShared: share != nil || !zone.isOwn,
                    permission: zone.permission == CKShare.ParticipantPermission.readOnly.rawValue ? .readOnly : .readWrite,
                    participants: participants, shareData: zone.share)
    }

    /// A zone as known here; nil for one that isn't, or that has been removed.
    public func zone(forKey key: String) -> Zone? {
        guard let zone = zones[key], zone.removedDate == nil else { return nil }
        return publicZone(zone)
    }

    /// Every zone known here other than the default one.
    public func allZones() -> [Zone] {
        zones.values.filter { $0.removedDate == nil }.map { publicZone($0) }
    }

    // MARK: - Shares

    internal static func decodeShare(_ data: Data) -> CKShare? {
        try? NSKeyedUnarchiver.unarchivedObject(ofClass: CKShare.self, from: data)
    }

    private static func encodeShare(_ share: CKShare) -> Data? {
        try? NSKeyedArchiver.archivedData(withRootObject: share, requiringSecureCoding: true)
    }

    /// Stores a share fetched or saved for a zone, and reports the change.
    private func updateShare(_ share: CKShare?, zoneID: CKRecordZone.ID) async {
        guard let key = zoneKey(for: zoneID), var zone = zones[key], zone.removedDate == nil else { return }
        let data = share.flatMap { Self.encodeShare($0) }
        let permission = share?.currentUserParticipant?.permission ?? .readWrite
        let permissionValue = (permission == .readOnly ? CKShare.ParticipantPermission.readOnly : .readWrite).rawValue
        guard zone.share != data || zone.permission != permissionValue else { return }
        zone.share = data
        zone.permission = permissionValue
        await write(zone: zone)
        await zoneEventHandler?(.shareChanged(publicZone(zone)))
    }

    /// The share of one of the user's own zones, made (and the zone with it, if it isn't on
    /// the server yet) when `creating` is set and it has none. Nil for a zone that isn't the
    /// user's own to share, or that has no share and isn't to get one.
    public func share(forZoneKey key: String, creating: Bool, details: ShareDetails? = nil) async throws -> CKShare? {
        guard let zone = zones[key], zone.removedDate == nil, zone.isOwn else { return nil }
        if let data = zone.share, let existing = Self.decodeShare(data) {
            // An existing share takes the details as they are now, if they've changed.
            guard let details, details.apply(to: existing) else { return existing }
            let saved = try await CKContainer.default().privateCloudDatabase.save(existing)
            if let savedShare = saved as? CKShare {
                await updateShare(savedShare, zoneID: zone.zoneID)
                return savedShare
            }
            return existing
        }
        guard creating else { return nil }
        let cloudDatabase = CKContainer.default().privateCloudDatabase
        _ = try await cloudDatabase.save(CKRecordZone(zoneID: zone.zoneID))
        let share = CKShare(recordZoneID: zone.zoneID)
        share.publicPermission = .none
        _ = details?.apply(to: share)
        let saved = try await cloudDatabase.save(share)
        if let savedShare = saved as? CKShare {
            await updateShare(savedShare, zoneID: zone.zoneID)
            return savedShare
        }
        return share
    }

    /// What a share's link says it opens: kept on the share record itself, which is what the
    /// system shows whoever opens the link. Without them, the system names the record type
    /// ("cloudkit.zoneshare").
    public struct ShareDetails: Sendable {
        public var title: String
        public var thumbnail: Data?
        /// A uniform type identifier for what's shared.
        public var type: String?

        public init(title: String, thumbnail: Data? = nil, type: String? = nil) {
            self.title = title
            self.thumbnail = thumbnail
            self.type = type
        }

        /// Sets the details on a share; returns whether anything changed.
        func apply(to share: CKShare) -> Bool {
            var changed = false
            if share[CKShare.SystemFieldKey.title] as? String != title {
                share[CKShare.SystemFieldKey.title] = title as CKRecordValue
                changed = true
            }
            if let thumbnail, share[CKShare.SystemFieldKey.thumbnailImageData] as? Data != thumbnail {
                share[CKShare.SystemFieldKey.thumbnailImageData] = thumbnail as CKRecordValue
                changed = true
            }
            if let type, share[CKShare.SystemFieldKey.shareType] as? String != type {
                share[CKShare.SystemFieldKey.shareType] = type as CKRecordValue
                changed = true
            }
            return changed
        }
    }

    /// Takes in a share saved by the sharing interface, so what's known here is current.
    public func noteShareSaved(_ share: CKShare) async {
        await updateShare(share, zoneID: share.recordID.zoneID)
    }

    /// Stops sharing one of the user's own zones: the share is deleted, and every participant
    /// loses the zone.
    public func stopSharing(zoneKey key: String) async throws {
        guard let zone = zones[key], zone.isOwn, let data = zone.share, let share = Self.decodeShare(data) else { return }
        _ = try await CKContainer.default().privateCloudDatabase.deleteRecord(withID: share.recordID)
        await updateShare(nil, zoneID: zone.zoneID)
    }

    /// Leaves a zone shared with the user: its share is deleted from the shared database, which
    /// takes the zone with it, and its rows go from here.
    public func leaveShare(zoneKey key: String) async throws {
        guard let zone = zones[key], zone.removedDate == nil, !zone.isOwn else { return }
        let shareID = CKRecord.ID(recordName: CKRecordNameZoneWideShare, zoneID: zone.zoneID)
        do {
            _ = try await CKContainer.default().sharedCloudDatabase.deleteRecord(withID: shareID)
        } catch let error as CKError where error.code == .unknownItem || error.code == .zoneNotFound {
            // Already gone.
        }
        await removeLocalZone(key: key, reason: .left)
    }

    /// Accepts a share the user was handed (a link they opened), and brings its zone in: the zone
    /// is known here at once, so the app can show it, and its records are fetched.
    public func acceptShare(_ metadata: CKShare.Metadata) async throws {
        let share = try await CKContainer.default().accept(metadata)
        let zoneID = share.recordID.zoneID
        guard let key = zoneKey(for: zoneID), !Self.isOwn(zoneID) else { return }
        if var zone = zones[key] {
            // Accepted again after leaving, or already known: back as a live zone.
            zone.removedDate = nil
            zone.ownerName = zoneID.ownerName
            await write(zone: zone)
        } else {
            await register(key: key, ownerName: zoneID.ownerName)
        }
        await updateShare(share, zoneID: zoneID)
        if let zone = zones[key] { await zoneEventHandler?(.added(publicZone(zone))) }
        try? await sharedEngine?.fetchChanges(CKSyncEngine.FetchChangesOptions(scope: .zoneIDs([zoneID])))
    }

    /// Takes in that the user stopped sharing one of their own zones through the sharing
    /// interface (which deleted the share itself).
    public func noteSharingStopped(zoneKey key: String) async {
        guard let zone = zones[key], zone.isOwn else { return }
        await updateShare(nil, zoneID: zone.zoneID)
    }

    /// Takes in that the user left a zone shared with them through the sharing interface (which
    /// removed them from the share itself): its rows go from here.
    public func noteLeftShare(zoneKey key: String) async {
        guard let zone = zones[key], !zone.isOwn else { return }
        await removeLocalZone(key: key, reason: .left)
    }

    /// Removes one of the user's own zones, on the server and here: everything in it is gone for
    /// good, on every device and for every participant. Rows the app deletes itself go one record
    /// at a time; this is how a whole document goes at once.
    public func removeZone(key: String) async {
        guard let zone = zones[key], zone.removedDate == nil, zone.isOwn else { return }
        await removeLocalZone(key: key, reason: .removed)
        privateEngine?.state.add(pendingDatabaseChanges: [.deleteZone(zone.zoneID)])
    }

    /// Fetches what's new in a zone now, rather than waiting for a push: for a shared document
    /// that's open, where seeing others' edits soon matters.
    public func fetchChanges(zoneKey key: String?) async {
        let zoneID: CKRecordZone.ID
        if let key {
            guard let zone = zones[key], zone.removedDate == nil else { return }
            zoneID = zone.zoneID
        } else {
            zoneID = Self.defaultZoneID
        }
        try? await engine(for: zoneID)?.fetchChanges(CKSyncEngine.FetchChangesOptions(scope: .zoneIDs([zoneID])))
    }

    /// Fetches what's new everywhere now.
    public func fetchChanges() async {
        try? await privateEngine?.fetchChanges()
        try? await sharedEngine?.fetchChanges()
    }

    /// Drops a zone from here: its rows, its queued changes, and its registration (kept as a
    /// tombstone for a while). Server-side, nothing: the caller has deleted the zone or the share,
    /// or the server has.
    private func removeLocalZone(key: String, reason: RemovalReason) async {
        guard let database, var zone = zones[key], zone.removedDate == nil else { return }
        zone.removedDate = Date()
        await write(zone: zone)
        let zoneID = zone.zoneID

        for model in models.values where model.skybridgeZoneColumnName() != nil {
            await deleteRows(model, zoneKey: key)
        }
        _ = try? await database.query("DELETE FROM `\(BlackbirdSkybridgeDirtyRow.tableName)` WHERE zoneValue = ?", key)
        _ = try? await database.query("DELETE FROM `\(BlackbirdSkybridgeQueuedChange.tableName)` WHERE zoneName = ? AND ownerName = ?", zoneID.zoneName, zoneID.ownerName)
        if let engine = engine(for: zoneID) {
            let pending = engine.state.pendingRecordZoneChanges.filter {
                switch $0 {
                    case .saveRecord(let id), .deleteRecord(let id): return id.zoneID == zoneID
                    @unknown default: return false
                }
            }
            engine.state.remove(pendingRecordZoneChanges: pending)
        }
        Self.logger.info("☁️ Skybridge removed zone \(key, privacy: .public): \(String(describing: reason), privacy: .public)")
        await zoneEventHandler?(.removed(key: key, reason: reason))
    }

    /// Deletes a zone's rows of a table, in batches, unmarked: they aren't local edits.
    private func deleteRows<T: BlackbirdSkybridgeSyncable>(_ type: T.Type, zoneKey: String) async {
        guard let database, let column = T.skybridgeZoneColumnName() else { return }
        let value = T.skybridgeZoneColumnValue(fromText: zoneKey)
        while true {
            let deleted: Int64 = (try? await database.transaction { core in
                try Self.suppressingTriggers(core) {
                    try core.query("DELETE FROM `\(T.tableName)` WHERE rowid IN (SELECT rowid FROM `\(T.tableName)` WHERE `\(column)` = ? LIMIT \(Self.batchSize))", value)
                    return core.lastStatementChangeCount
                }
            }) ?? 0
            if deleted < Self.batchSize { return }
            await Task.yield()
        }
    }

    // MARK: - Pending changes
    //
    // An engine's pending list is serialized whole on every state update, so it is kept to a
    // bounded slice; everything beyond that waits in the queue table and is fed in as sends
    // complete. Changes that fit go straight to the engine. Each change goes to the engine of
    // its zone's database: the user's own zones to the private one, others' to the shared one.

    private static func recordID(of change: CKSyncEngine.PendingRecordZoneChange) -> CKRecord.ID? {
        switch change {
            case .saveRecord(let id), .deleteRecord(let id): return id
            @unknown default: return nil
        }
    }

    private func enqueue(_ changes: [CKSyncEngine.PendingRecordZoneChange]) async {
        guard let database, !changes.isEmpty else { return }
        let byEngine = Dictionary(grouping: changes) { Self.recordID(of: $0).map { engineID(for: $0.zoneID) } ?? -1 }
        for (id, changes) in byEngine {
            guard let engine = id == BlackbirdSkybridgeState.privateEngineID ? privateEngine : id == BlackbirdSkybridgeState.sharedEngineID ? sharedEngine : nil else { continue }
            let room = max(0, Self.engineSliceLimit - engine.state.pendingRecordZoneChanges.count)
            let direct = Array(changes.prefix(room))
            let queued = Array(changes.dropFirst(room))
            if !direct.isEmpty { engine.state.add(pendingRecordZoneChanges: direct) }
            if !queued.isEmpty {
                let rows = queued.compactMap { change -> BlackbirdSkybridgeQueuedChange? in
                    guard let id = Self.recordID(of: change) else { return nil }
                    if case .deleteRecord = change {
                        return BlackbirdSkybridgeQueuedChange(recordName: id.recordName, isDeletion: true, zoneName: id.zoneID.zoneName, ownerName: id.zoneID.ownerName)
                    }
                    return BlackbirdSkybridgeQueuedChange(recordName: id.recordName, isDeletion: false, zoneName: id.zoneID.zoneName, ownerName: id.zoneID.ownerName)
                }
                for chunk in rows.chunked(into: Self.batchSize) {
                    try? await database.transaction { core in
                        for row in chunk { try row.write(to: core) }
                    }
                }
            }
        }
    }

    /// Moves queued changes into the engines while they have room.
    private func topUpPendingChanges() async {
        await topUpPendingChanges(engineID: BlackbirdSkybridgeState.privateEngineID)
        await topUpPendingChanges(engineID: BlackbirdSkybridgeState.sharedEngineID)
    }

    private func topUpPendingChanges(engineID: Int) async {
        let isPrivate = engineID == BlackbirdSkybridgeState.privateEngineID
        guard let engine = isPrivate ? privateEngine : sharedEngine, let database, !toppingUp.contains(engineID) else { return }
        toppingUp.insert(engineID)
        defer { toppingUp.remove(engineID) }
        let ownerCondition = isPrivate ? "ownerName = ?" : "ownerName != ?"
        while engine.state.pendingRecordZoneChanges.count < Self.engineSliceLimit {
            let room = Self.engineSliceLimit - engine.state.pendingRecordZoneChanges.count
            let limit = min(room, Self.batchSize)
            guard let rows = try? await BlackbirdSkybridgeQueuedChange.read(from: database, sqlWhere: "\(ownerCondition) ORDER BY rowid LIMIT \(limit)", CKCurrentUserDefaultName), !rows.isEmpty else { return }
            let changes: [CKSyncEngine.PendingRecordZoneChange] = rows.map {
                let id = CKRecord.ID(recordName: $0.recordName, zoneID: CKRecordZone.ID(zoneName: $0.zoneName, ownerName: $0.ownerName))
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
            await reconcileRows(model, primaryKeys: primaryKeys.compactMap(\.first))
        } else {
            // Unknown scope of changes: re-check every row, and whatever was deleted.
            await reconcileAllRows(model)
            await reconcileDirtyRows(tableName: tableName)
        }
    }

    /// Clears markers once their rows are reconciled: an edit's marker only if it is still an
    /// edit's, so a deletion made meanwhile isn't forgotten, and the other way round.
    private func clearDirty(recordNames: [String], deletions: Bool) async {
        guard let database, !recordNames.isEmpty else { return }
        for chunk in recordNames.chunked(into: Self.batchSize) {
            let placeholders = Array(repeating: "?", count: chunk.count).joined(separator: ",")
            _ = try? await database.query("DELETE FROM `\(BlackbirdSkybridgeDirtyRow.tableName)` WHERE isDeletion = ? AND recordName IN (\(placeholders))", arguments: [deletions ? 1 : 0] + chunk)
        }
    }

    /// Reconciles every marked row, of one table or all of them.
    private func reconcileDirtyRows(tableName: String?) async {
        guard let database else { return }
        let dirty: [BlackbirdSkybridgeDirtyRow]
        if let tableName {
            dirty = (try? await BlackbirdSkybridgeDirtyRow.read(from: database, matching: \.$tableName == tableName)) ?? []
        } else {
            dirty = (try? await BlackbirdSkybridgeDirtyRow.read(from: database, matching: .all)) ?? []
        }
        guard !dirty.isEmpty else { return }
        Self.logger.info("☁️ Skybridge reconciling \(dirty.count) rows left dirty")
        let byTable = Dictionary(grouping: dirty, by: \.tableName)
        for (tableName, rows) in byTable {
            guard let model = models[tableName] else {
                await clearDirty(recordNames: rows.map(\.recordName), deletions: false)
                await clearDirty(recordNames: rows.map(\.recordName), deletions: true)
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
            let rows = (try? await Self.rawRows(T.self, primaryKeys: chunk, in: database)) ?? []
            var found = Set<Blackbird.Value>()
            for row in rows {
                guard let key = row[Self.primaryKeyName(T.self)] else { continue }
                found.insert(key)
                if Self.isClean(row, for: T.self) {
                    if let keyString = key.stringValue { cleanNames.append(recordName(tableName: T.tableName, primaryKeyString: keyString)) }
                } else {
                    changed.append(key)
                }
            }
            changed += chunk.filter { !found.contains($0) } // missing rows: deleted locally
        }
        await clearDirty(recordNames: cleanNames, deletions: false)
        for key in changed {
            await reconcileLocalRow(type, primaryKey: key)
        }
    }

    /// Whether a row matches its server base in the current metadata format, without a transaction.
    private static func isClean<T: BlackbirdSkybridgeSyncable>(_ row: Blackbird.Row, for type: T.Type) -> Bool {
        let metadata = metadata(of: row)
        guard !metadata.needsUpgrade, let fingerprint = metadata.syncedFingerprint else { return false }
        return localFingerprint(of: row, for: T.self, metadata: metadata) == fingerprint
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
                guard let key = plain[Self.primaryKeyName(T.self)] else { continue }
                if !Self.isClean(plain, for: T.self) { changed.append(key) }
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
            var stale: [(key: Blackbird.Value, metadata: SkybridgeMetadata)] = []
            for row in rows {
                guard let rowID = row["__rowid"]?.int64Value else { continue }
                lastRowID = max(lastRowID, rowID)
                if case .data(let data) = row["__meta"] ?? .null, let key = row["__pk"] {
                    let metadata = SkybridgeMetadata.decode(data)
                    if metadata.needsUpgrade { stale.append((key, metadata)) }
                }
            }
            if !stale.isEmpty {
                let batch = stale
                try? await database.transaction { core in
                    try Self.suppressingTriggers(core) {
                        for (key, metadata) in batch {
                            var upgraded = metadata
                            if Self.upgrade(&upgraded, for: T.self) { try Self.writeMetadata(upgraded, for: T.self, primaryKey: key, in: core) }
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
        case ignore
        case upload(zoneKey: String?)
        case deleteFromServer(zoneKey: String?)
        /// The row belongs to a zone that's gone: a stale editor wrote it. It goes.
        case dropStraggler
    }

    /// Compares a row against its server base and schedules an upload (or a deletion) if they
    /// differ. Rows that match are echoes of changes this engine just applied, and are ignored.
    private func reconcileLocalRow<T: BlackbirdSkybridgeSyncable>(_ type: T.Type, primaryKey: Blackbird.Value) async {
        guard let database, let primaryKeyString = primaryKey.stringValue else { return }
        let recordName = recordName(tableName: T.tableName, primaryKeyString: primaryKeyString)
        let removedZoneKeys = Set(zones.values.filter { $0.removedDate != nil }.map(\.key))

        // The read, comparison, and timestamp bump happen in one transaction so a
        // concurrent writer can't be overwritten by a stale full-row rewrite.
        let action: ReconcileAction = (try? await database.transaction { core in
            guard let row = try Self.rawRow(T.self, primaryKey: primaryKey, in: core) else {
                // Deleted here. Its marker says which zone it was in; without one, the deletion
                // was Skybridge's own (a server deletion applied), and there's nothing to send.
                guard let marker = try BlackbirdSkybridgeDirtyRow.read(from: core, primaryKey: recordName), marker.isDeletion else { return ReconcileAction.ignore }
                let zoneKey = T.skybridgeZoneKey(fromText: marker.zoneValue)
                if let zoneKey, removedZoneKeys.contains(zoneKey) { return ReconcileAction.ignore }
                return ReconcileAction.deleteFromServer(zoneKey: zoneKey)
            }
            let zoneKey = T.skybridgeZoneKey(of: row)
            if let zoneKey, removedZoneKeys.contains(zoneKey) {
                try Self.suppressingTriggers(core) {
                    if let instance = try T.read(from: core, primaryKey: primaryKey) { try instance.delete(from: core) }
                }
                return ReconcileAction.dropStraggler
            }

            var metadata = Self.metadata(of: row)
            var metadataChanged = Self.upgrade(&metadata, for: T.self)
            if let fingerprint = metadata.syncedFingerprint, Self.localFingerprint(of: row, for: T.self, metadata: metadata) == fingerprint {
                if metadataChanged { try Self.suppressingTriggers(core) { try Self.writeMetadata(metadata, for: T.self, primaryKey: primaryKey, in: core) } }
                return ReconcileAction.ignore
            }

            // A genuine local edit: bump the last-write-wins timestamp if the
            // writer didn't. (The rewrite re-enters here once, then no-ops.)
            // Compared as TimeIntervals: the iOS 27.2 SDK back-deploys Date.<= and >= to an OS
            // symbol that iOS 27.2 betas lack, so calling them there jumps to null and crashes.
            let lastSyncedModified = metadata.syncedLastModifiedDate
            let localModified = metadata.syncLastModifiedDate
            if localModified == nil || (lastSyncedModified != nil && localModified!.timeIntervalSinceReferenceDate <= lastSyncedModified!.timeIntervalSinceReferenceDate) {
                metadata.syncLastModifiedDate = Date()
                metadataChanged = true
            }
            if metadataChanged { try Self.suppressingTriggers(core) { try Self.writeMetadata(metadata, for: T.self, primaryKey: primaryKey, in: core) } }
            return ReconcileAction.upload(zoneKey: zoneKey)
        }) ?? .ignore // a failed read/write here must not become a server deletion

        switch action {
            case .ignore:
                await clearDirty(recordNames: [recordName], deletions: false)
            case .dropStraggler:
                Self.logger.info("☁️ Skybridge dropped \(recordName, privacy: .public): its zone is gone")
                await clearDirty(recordNames: [recordName], deletions: false)
            case .upload(let zoneKey):
                if let zoneID = await zoneIDForUpload(key: zoneKey) {
                    await enqueue([.saveRecord(CKRecord.ID(recordName: recordName, zoneID: zoneID))])
                }
                await clearDirty(recordNames: [recordName], deletions: false)
            case .deleteFromServer(let zoneKey):
                if let zoneID = await zoneIDForUpload(key: zoneKey) {
                    await enqueue([.deleteRecord(CKRecord.ID(recordName: recordName, zoneID: zoneID))])
                }
                await clearDirty(recordNames: [recordName], deletions: true)
        }
    }

    // MARK: - Server changes

    /// A fetched record reduced to Sendable parts, so it can be applied inside a transaction.
    private struct ServerRecord: Sendable {
        let recordName: String
        let zoneName: String
        let ownerName: String
        let zoneKey: String?
        let primaryKey: Blackbird.Value
        let serverModified: Date
        let systemFields: Data?
        let fingerprint: Data
        let assetDigests: [String: String]
        let values: [(name: String, value: Blackbird.Value)]
        let pendingAssetColumns: [String]

        var zoneID: CKRecordZone.ID { CKRecordZone.ID(zoneName: zoneName, ownerName: ownerName) }
        var recordID: CKRecord.ID { CKRecord.ID(recordName: recordName, zoneID: zoneID) }
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
            zoneName: record.recordID.zoneID.zoneName,
            ownerName: record.recordID.zoneID.ownerName,
            zoneKey: zoneKey(for: record.recordID.zoneID),
            primaryKey: primaryKey,
            serverModified: record.encryptedValues[Field.lastModified] as? Date ?? record.modificationDate ?? .distantPast,
            systemFields: Self.encodeSystemFields(record),
            fingerprint: Self.recordFingerprint(record, for: T.self),
            assetDigests: Self.assetDigests(from: record, for: T.self),
            values: values,
            pendingAssetColumns: pending
        )
    }

    /// Applies fetched records in one transaction per batch, keeping the latest write of each
    /// (or what the type's merge makes of the two): if the local row was modified more recently,
    /// its values win and are re-uploaded on top of the server record; otherwise the server
    /// values are applied locally. With `forceServer`, the server's values are taken regardless:
    /// the row is being put back as the server has it.
    ///
    /// A record is applied only to a row of its own zone: a row that lives in another zone here
    /// (one still in the default zone from before zones, say) is left alone, as is a record whose
    /// own zone column disagrees with the zone it came from.
    private func applyServerRecords<T: BlackbirdSkybridgeSyncable>(_ type: T.Type, records: [ServerRecord], forceServer: Bool = false) async {
        guard let database, !records.isEmpty else { return }
        let zoneColumn = T.skybridgeZoneColumnName()
        for chunk in records.chunked(into: Self.batchSize) {
            // A record deleted here, whose deletion the server hasn't taken yet, is stale however
            // it arrives: the echo of its save from before the deletion, usually. Put back, it
            // would be deleted again once the deletion lands, and meanwhile it's ink the user
            // undid. The same for one fetched from before a deletion the server just confirmed.
            let deletedHere = forceServer ? [] : await deletionsPending(among: chunk.map(\.recordName))
            let recentlyDeleted = forceServer ? [:] : recentDeletions
            let outcome: (localWins: [ServerRecord], pendingRefetch: [ServerRecord]) = (try? await database.transaction { core in
                var localWins: [ServerRecord] = []
                var pendingRefetch: [ServerRecord] = []
                try Self.suppressingTriggers(core) {
                    for server in chunk {
                        if deletedHere.contains(server.recordName) {
                            Self.logger.info("☁️ Skybridge: \(server.recordName, privacy: .public) arrived while its deletion is on its way; ignored")
                            continue
                        }
                        if let zoneColumn {
                            // The zone column may be the primary key itself, which travels in the record name.
                            let zoneValue = zoneColumn == Self.primaryKeyName(T.self) ? server.primaryKey : server.values.first { $0.name == zoneColumn }?.value ?? .null
                            let recordZoneKey = T.skybridgeZoneKey(for: zoneValue)
                            guard recordZoneKey == server.zoneKey else {
                                Self.logger.error("☁️🛑 Skybridge: \(server.recordName, privacy: .public) from zone \(server.zoneName, privacy: .public) says it belongs in \(recordZoneKey ?? "the default zone", privacy: .public); ignored")
                                continue
                            }
                        }
                        let existing = try Self.rawRow(T.self, primaryKey: server.primaryKey, in: core)

                        if existing == nil, !forceServer {
                            // Deleted here and not yet reconciled: the marker says so, and the
                            // deletion goes to the server from there.
                            if let marker = try BlackbirdSkybridgeDirtyRow.read(from: core, primaryKey: server.recordName), marker.isDeletion { continue }
                            if let deleted = recentlyDeleted[server.recordName], server.serverModified.timeIntervalSinceReferenceDate < deleted.timeIntervalSinceReferenceDate {
                                Self.logger.info("☁️ Skybridge: \(server.recordName, privacy: .public) arrived from before its deletion here; ignored")
                                continue
                            }
                        }

                        if let existing {
                            guard T.skybridgeZoneKey(of: existing) == server.zoneKey else { continue }
                            var metadata = Self.metadata(of: existing)
                            if !forceServer, let synced = metadata.syncedFingerprint, synced == server.fingerprint {
                                // The record as this row last synced it: the echo of its own
                                // upload, or a save elsewhere of the same values. Nothing in it is
                                // newer than the row, so any edit made here since stands; only
                                // the server's system fields are taken, so the next upload builds
                                // on the record as it is now.
                                metadata.ckRecordData = nil
                                metadata.setSystemFieldsArchive(server.systemFields, recordType: T.tableName)
                                metadata.syncedLastModifiedDate = server.serverModified
                                try Self.writeMetadata(metadata, for: T.self, primaryKey: server.primaryKey, in: core)
                                continue
                            }
                            let hasLocalEdits = !forceServer && (metadata.syncedFingerprint == nil || Self.localFingerprint(of: existing, for: T.self, metadata: metadata) != metadata.syncedFingerprint!)
                            var localModified = metadata.syncLastModifiedDate
                            if hasLocalEdits, let synced = metadata.syncedLastModifiedDate, localModified == nil || localModified!.timeIntervalSinceReferenceDate <= synced.timeIntervalSinceReferenceDate {
                                // Edited since its last sync, but not reconciled yet (the record
                                // arrived between the write and its change report), so the edit
                                // hasn't been stamped: it's as new as reconcile would stamp it.
                                localModified = Date()
                            }
                            let localIsNewer = !forceServer && localModified != nil && localModified! > server.serverModified
                            if hasLocalEdits, !metadata.needsUpgrade {
                                let names = T.skybridgeSyncedColumnNames()
                                var localRow = Blackbird.Row()
                                for name in names { localRow[name] = existing[name] ?? .null }
                                var serverRow = Blackbird.Row()
                                for (name, value) in server.values where names.contains(name) { serverRow[name] = value }
                                if let merged = T.skybridgeMerge(local: localRow, server: serverRow, localIsNewer: localIsNewer) {
                                    try Self.applyMerged(merged, server: server, metadata: metadata, for: T.self, in: core)
                                    continue
                                }
                            }
                            if localIsNewer {
                                // Local wins: keep local values, adopt the server record as the new base, re-send.
                                metadata.ckRecordData = nil
                                metadata.setSystemFieldsArchive(server.systemFields, recordType: T.tableName)
                                metadata.syncedFingerprint = server.fingerprint
                                metadata.assetDigests = server.assetDigests.isEmpty ? nil : server.assetDigests
                                metadata.syncedLastModifiedDate = server.serverModified
                                try Self.writeMetadata(metadata, for: T.self, primaryKey: server.primaryKey, in: core)
                                localWins.append(server)
                                continue
                            }
                        }

                        // Server wins (or no local row): the server's values go into the row exactly as
                        // they came, bypassing the model, which would decode a value it doesn't know as nil.
                        var metadata = SkybridgeMetadata()
                        metadata.setSystemFieldsArchive(server.systemFields, recordType: T.tableName)
                        metadata.syncedFingerprint = server.fingerprint
                        metadata.assetDigests = server.assetDigests.isEmpty ? nil : server.assetDigests
                        metadata.syncLastModifiedDate = server.serverModified
                        metadata.syncedLastModifiedDate = server.serverModified
                        metadata.pendingAssetColumns = server.pendingAssetColumns.isEmpty ? nil : server.pendingAssetColumns
                        let columns = server.values + [(name: skybridgeMetadataColumnName, value: Self.value(of: metadata))]
                        if existing != nil {
                            try Self.update(T.self, primaryKey: server.primaryKey, columns: columns, in: core)
                        } else {
                            try Self.insert(T.self, primaryKey: server.primaryKey, columns: columns, in: core)
                        }
                        if !server.pendingAssetColumns.isEmpty { pendingRefetch.append(server) }
                    }
                }
                return (localWins, pendingRefetch)
            }) ?? ([], [])

            if !outcome.localWins.isEmpty {
                await enqueue(outcome.localWins.map { .saveRecord($0.recordID) })
            }
            for server in outcome.pendingRefetch { scheduleAssetRefetch(recordID: server.recordID) }
        }
    }

    /// The names, among some records', whose deletion from the server is on its way from here:
    /// waiting in an engine or the queue table, or sent and not yet answered. (A deletion marked
    /// but not yet reconciled is checked where the row is read, in the same transaction.)
    private func deletionsPending(among names: [String]) async -> Set<String> {
        guard let database, !names.isEmpty else { return [] }
        let wanted = Set(names)
        var pending = inFlightDeletions.intersection(wanted)
        for engine in [privateEngine, sharedEngine].compactMap({ $0 }) {
            for change in engine.state.pendingRecordZoneChanges {
                if case .deleteRecord(let id) = change, wanted.contains(id.recordName) { pending.insert(id.recordName) }
            }
        }
        let placeholders = names.map { _ in "?" }.joined(separator: ",")
        let queued = (try? await BlackbirdSkybridgeQueuedChange.read(from: database, sqlWhere: "isDeletion = 1 AND recordName IN (\(placeholders))", arguments: names)) ?? []
        pending.formUnion(queued.map(\.recordName))
        return pending
    }

    /// Writes a merge's result over the server record as its base. Where the result differs
    /// from the server's values it is a new edit, stamped later than the server's write so it
    /// wins everywhere; the write's change report brings it back through reconcile and uploads it.
    private static func applyMerged<T: BlackbirdSkybridgeSyncable>(_ merged: Blackbird.Row, server: ServerRecord, metadata: SkybridgeMetadata, for type: T.Type, in core: isolated Blackbird.Database.Core) throws {
        var metadata = metadata
        metadata.ckRecordData = nil
        metadata.setSystemFieldsArchive(server.systemFields, recordType: T.tableName)
        metadata.syncedFingerprint = server.fingerprint
        metadata.assetDigests = server.assetDigests.isEmpty ? nil : server.assetDigests
        metadata.syncedLastModifiedDate = server.serverModified
        metadata.pendingAssetColumns = server.pendingAssetColumns.isEmpty ? nil : server.pendingAssetColumns
        let names = T.skybridgeSyncedColumnNames()
        let sameAsServer = names.allSatisfy { name in canonical(merged[name] ?? .null) == canonical(server.values.first { $0.name == name }?.value ?? .null) }
        metadata.syncLastModifiedDate = sameAsServer ? server.serverModified : max(Date(), server.serverModified.addingTimeInterval(0.001))
        var columns: [(name: String, value: Blackbird.Value)] = names.map { ($0, merged[$0] ?? .null) }
        columns += server.values.filter { !names.contains($0.name) } // asset columns, as fetched
        columns.append((skybridgeMetadataColumnName, value(of: metadata)))
        try update(T.self, primaryKey: server.primaryKey, columns: columns, in: core)
    }

    private func applyServerRecord(_ record: CKRecord, forceServer: Bool = false) async {
        guard let (model, primaryKey) = modelAndPrimaryKey(for: record.recordID) else { return }
        await applyFetchedRecords(model, records: [record], primaryKeys: [primaryKey], forceServer: forceServer)
    }

    private func applyFetchedRecords<T: BlackbirdSkybridgeSyncable>(_ type: T.Type, records: [CKRecord], primaryKeys: [Blackbird.Value], forceServer: Bool = false) async {
        let extracted = zip(records, primaryKeys).map { serverRecord(from: $0, for: T.self, primaryKey: $1) }
        await applyServerRecords(T.self, records: extracted, forceServer: forceServer)
    }

    private func handleFetchedRecordZoneChanges(_ changes: CKSyncEngine.Event.FetchedRecordZoneChanges) async {
        // Group by table so each batch goes through one generic, one transaction.
        var byTable: [String: (records: [CKRecord], keys: [Blackbird.Value])] = [:]
        for modification in changes.modifications {
            let record = modification.record
            let zoneID = record.recordID.zoneID
            // A record of a zone that can't be synced (someone else's default zone) is ignored.
            if zoneKey(for: zoneID) != nil, await registeredZone(for: zoneID) == nil { continue }
            if let share = record as? CKShare {
                await updateShare(share, zoneID: zoneID)
                continue
            }
            guard let (model, primaryKey) = modelAndPrimaryKey(for: record.recordID) else { continue }
            byTable[model.tableName, default: ([], [])].records.append(record)
            byTable[model.tableName, default: ([], [])].keys.append(primaryKey)
        }
        for (tableName, group) in byTable {
            guard let model = models[tableName] else { continue }
            await applyFetchedRecords(model, records: group.records, primaryKeys: group.keys)
        }

        var deletionsByTable: [String: [ServerDeletion]] = [:]
        for deletion in changes.deletions {
            if deletion.recordType == CKRecord.SystemType.share {
                await updateShare(nil, zoneID: deletion.recordID.zoneID)
                continue
            }
            guard let (model, primaryKey) = modelAndPrimaryKey(for: deletion.recordID) else { continue }
            deletionsByTable[model.tableName, default: []].append(ServerDeletion(recordName: deletion.recordID.recordName, zoneKey: zoneKey(for: deletion.recordID.zoneID), key: primaryKey))
        }
        for (tableName, deletions) in deletionsByTable {
            guard let model = models[tableName] else { continue }
            await applyServerDeletions(model, deletions: deletions)
        }

        let applied = changes.modifications.count + changes.deletions.count
        if applied > 0 { await fetchActivityHandler?(.applied(records: applied)) }
    }

    private struct ServerDeletion: Sendable {
        let recordName: String
        let zoneKey: String?
        let key: Blackbird.Value
    }

    /// Deletes rows the server deleted: only rows of the record's own zone, so a record deleted
    /// from a zone a row no longer lives in doesn't take the row with it.
    ///
    /// A row never confirmed with this server (no server base) isn't the record that was deleted:
    /// it was written here, or brought from another environment, and hasn't been uploaded yet. It
    /// stays, and is sent, recreating the record.
    private func applyServerDeletions<T: BlackbirdSkybridgeSyncable>(_ type: T.Type, deletions: [ServerDeletion]) async {
        guard let database else { return }
        var kept: [ServerDeletion] = []
        for chunk in deletions.chunked(into: Self.batchSize) {
            let keptInChunk: [ServerDeletion] = (try? await database.transaction { core in
                try Self.suppressingTriggers(core) {
                    var keptInChunk: [ServerDeletion] = []
                    for deletion in chunk {
                        guard let row = try Self.rawRow(T.self, primaryKey: deletion.key, in: core), T.skybridgeZoneKey(of: row) == deletion.zoneKey else { continue }
                        if !Self.metadata(of: row).hasServerBase {
                            keptInChunk.append(deletion)
                            continue
                        }
                        if let instance = try T.read(from: core, primaryKey: deletion.key) { try instance.delete(from: core) }
                    }
                    return keptInChunk
                }
            }) ?? []
            kept += keptInChunk
        }
        guard !kept.isEmpty else { return }
        Self.logger.info("☁️ Skybridge kept \(kept.count) \(T.tableName, privacy: .public) rows the server deleted but this device never confirmed; resending")
        var changes: [CKSyncEngine.PendingRecordZoneChange] = []
        for deletion in kept {
            guard let zoneID = await zoneIDForUpload(key: deletion.zoneKey) else { continue }
            changes.append(.saveRecord(CKRecord.ID(recordName: deletion.recordName, zoneID: zoneID)))
        }
        await enqueue(changes)
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

    private func refetchRecord(recordID: CKRecord.ID, forceServer: Bool = false) async {
        guard let record = try? await cloudDatabase(for: recordID.zoneID).record(for: recordID) else { return }
        await applyServerRecord(record, forceServer: forceServer)
    }

    private func refetchRecordsWithPendingAssets() async {
        for model in models.values {
            await refetchRecordsWithPendingAssets(model)
        }
    }

    private func refetchRecordsWithPendingAssets<T: BlackbirdSkybridgeSyncable>(_ type: T.Type) async {
        guard let database, !T.skybridgeAssetColumns.isEmpty else { return }
        let zoneColumn = T.skybridgeZoneColumnName()
        var lastRowID: Int64 = 0
        while true {
            let zoneSelect = zoneColumn.map { ", `\($0)` AS __zone" } ?? ""
            let rows = (try? await T.query(in: database, "SELECT rowid AS __rowid, `\(Self.primaryKeyName(T.self))` AS __pk, `\(skybridgeMetadataColumnName)` AS __meta\(zoneSelect) FROM `\(T.tableName)` WHERE rowid > ? ORDER BY rowid LIMIT \(Self.batchSize)", lastRowID)) ?? []
            guard !rows.isEmpty else { return }
            for row in rows {
                guard let rowID = row["__rowid"]?.int64Value else { continue }
                lastRowID = max(lastRowID, rowID)
                guard case .data(let data) = row["__meta"] ?? .null,
                      let pending = SkybridgeMetadata.decode(data).pendingAssetColumns, !pending.isEmpty,
                      let keyString = row["__pk"]?.stringValue else { continue }
                let zoneKey = zoneColumn == nil ? nil : T.skybridgeZoneKey(for: row["__zone"] ?? .null)
                guard let zoneID = await zoneIDForUpload(key: zoneKey) else { continue }
                await refetchRecord(recordID: CKRecord.ID(recordName: recordName(tableName: T.tableName, primaryKeyString: keyString), zoneID: zoneID))
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
            return try await database.transaction { core in
                try Self.suppressingTriggers(core) {
                    guard let row = try Self.rawRow(T.self, primaryKey: primaryKey, in: core) else { return false }
                    var metadata = Self.metadata(of: row)
                    metadata.ckRecordData = nil
                    metadata.setSystemFieldsArchive(systemFields, recordType: T.tableName)
                    metadata.syncedFingerprint = fingerprint
                    metadata.assetDigests = digests.isEmpty ? nil : digests
                    metadata.syncedLastModifiedDate = syncedModified
                    // Columns that now hold local bytes are no longer awaiting a
                    // download; NULL columns stay pending until a refetch succeeds.
                    if let pending = metadata.pendingAssetColumns, !pending.isEmpty {
                        let stillPending = pending.filter { name in
                            if case .data = row[name] ?? .null { return false }
                            return true
                        }
                        metadata.pendingAssetColumns = stillPending.isEmpty ? nil : stillPending
                    }
                    try Self.writeMetadata(metadata, for: T.self, primaryKey: primaryKey, in: core)
                    return true
                }
            }
        } catch {
            return false
        }
    }

    private func handleSentRecordZoneChanges(_ event: CKSyncEngine.Event.SentRecordZoneChanges, engine: CKSyncEngine) async {
        let now = Date()
        recentDeletions = recentDeletions.filter { now.timeIntervalSince($0.value) < Self.recentDeletionWindow }
        for recordID in event.deletedRecordIDs {
            inFlightDeletions.remove(recordID.recordName)
            recentDeletions[recordID.recordName] = now
        }
        for (recordID, error) in event.failedRecordDeletes {
            inFlightDeletions.remove(recordID.recordName)
            // Already gone from the server: as good as deleted from here.
            if error.code == .unknownItem { recentDeletions[recordID.recordName] = now }
        }

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
                    await handleMissingZone(recordID.zoneID, resending: recordID)
                case .unknownItem:
                    // Deleted on the server. The type says whether our edit recreates it, or goes with it.
                    if let (model, _) = modelAndPrimaryKey(for: recordID), model.skybridgeServerDeletionWins {
                        await deleteLocally(recordID)
                    } else {
                        await clearServerRecordAndResend(recordID: recordID)
                    }
                case .permissionFailure:
                    // A read-only participant's edit: the server's version is put back.
                    Self.logger.error("☁️🚫 Skybridge: no permission to save \(recordID.recordName, privacy: .public); reverting")
                    await revertToServer(recordID)
                case .batchRequestFailed, .quotaExceeded, .requestRateLimited:
                    // Put back, in case failed saves are removed from the pending queue (adding a
                    // change that's still pending changes nothing). batchRequestFailed just means
                    // another record in the atomic batch failed; quotaExceeded records send once
                    // the account has space again; throttled ones once the throttle lifts.
                    engine.state.add(pendingRecordZoneChanges: [.saveRecord(recordID)])
                case .networkFailure, .networkUnavailable, .zoneBusy, .serviceUnavailable, .notAuthenticated, .operationCancelled:
                    break // Transient; the sync engine retries automatically.
                default:
                    Self.logger.error("☁️🛑 Skybridge: unhandled save failure for \(recordID.recordName, privacy: .public): \(failedSave.error, privacy: .public)")
            }
        }

        for (recordID, error) in event.failedRecordDeletes {
            switch error.code {
                case .permissionFailure:
                    // A read-only participant's deletion: the record comes back.
                    Self.logger.error("☁️🚫 Skybridge: no permission to delete \(recordID.recordName, privacy: .public); restoring")
                    await refetchRecord(recordID: recordID, forceServer: true)
                case .zoneNotFound:
                    await handleMissingZone(recordID.zoneID, resending: nil)
                default:
                    break // Already gone from the server, or transient.
            }
        }

        let engineID = engine === privateEngine ? BlackbirdSkybridgeState.privateEngineID : BlackbirdSkybridgeState.sharedEngineID
        lastSendProgress[engineID] = Date()
        await topUpPendingChanges(engineID: engineID)
    }

    /// An engine that's been throttled doesn't reliably send again once the throttle lifts: it can
    /// sit on its pending changes until something unrelated, like the next launch, starts a send.
    /// So every so often, an engine holding changes that hasn't sent any lately is asked to.
    private func startSendWatchdog() {
        guard sendWatchdog == nil else { return }
        sendWatchdog = Task(priority: .utility) { [weak self] in
            while !Task.isCancelled {
                var delay = Self.sendStallInterval
                if let wait = await self?.sendIfStalled() { delay = max(delay, wait) }
                try? await Task.sleep(for: .seconds(delay))
            }
        }
    }

    /// Asks any stalled engine to send. Returns how long a throttle asks to wait, if one did.
    private func sendIfStalled() async -> TimeInterval? {
        var wait: TimeInterval?
        for (engineID, engine) in [(BlackbirdSkybridgeState.privateEngineID, privateEngine), (BlackbirdSkybridgeState.sharedEngineID, sharedEngine)] {
            guard let engine else { continue }
            await topUpPendingChanges(engineID: engineID)
            let pending = engine.state.pendingRecordZoneChanges.count + engine.state.pendingDatabaseChanges.count
            guard pending > 0 else { continue }
            if let last = lastSendProgress[engineID], last > Date(timeIntervalSinceNow: -Self.sendStallInterval) { continue }
            Self.logger.info("☁️ Skybridge: \(pending) changes waiting with no send lately; sending")
            do {
                try await engine.sendChanges()
                lastSendProgress[engineID] = Date()
            } catch let error as CKError {
                Self.logger.error("☁️🛑 Skybridge: send failed: \(error, privacy: .public)")
                if let retryAfter = error.retryAfterSeconds { wait = max(wait ?? 0, retryAfter) }
            } catch {
                Self.logger.error("☁️🛑 Skybridge: send failed: \(error, privacy: .public)")
            }
        }
        return wait
    }

    /// A zone a change was sent to doesn't exist: one of the user's own that's yet to be created
    /// (or was deleted elsewhere and is still wanted here) is created and the change sent again;
    /// someone else's is gone for good, so its rows go.
    private func handleMissingZone(_ zoneID: CKRecordZone.ID, resending recordID: CKRecord.ID?) async {
        if Self.isOwn(zoneID) {
            if let key = zoneKey(for: zoneID), zones[key]?.removedDate != nil { return }
            privateEngine?.state.add(pendingDatabaseChanges: [.saveZone(CKRecordZone(zoneID: zoneID))])
            if let recordID { await clearServerRecordAndResend(recordID: recordID) }
        } else if let key = zoneKey(for: zoneID) {
            await removeLocalZone(key: key, reason: .shareRevoked)
        }
    }

    /// Discards stale server metadata for a row and schedules a fresh upload.
    private func clearServerRecordAndResend(recordID: CKRecord.ID) async {
        guard let (model, primaryKey) = modelAndPrimaryKey(for: recordID) else { return }
        if await storeServerBase(model, primaryKey: primaryKey, savedRecord: nil) {
            await enqueue([.saveRecord(recordID)])
        }
    }

    /// Puts a row back as the server has it, or removes it if the server has no such record.
    private func revertToServer(_ recordID: CKRecord.ID) async {
        do {
            let record = try await cloudDatabase(for: recordID.zoneID).record(for: recordID)
            await applyServerRecord(record, forceServer: true)
        } catch let error as CKError where error.code == .unknownItem {
            await deleteLocally(recordID)
        } catch {
            Self.logger.error("☁️🛑 Skybridge: couldn't fetch \(recordID.recordName, privacy: .public) to revert it: \(error, privacy: .public)")
        }
    }

    /// Deletes a row as the server deleted its record, unmarked.
    private func deleteLocally(_ recordID: CKRecord.ID) async {
        guard let (model, primaryKey) = modelAndPrimaryKey(for: recordID) else { return }
        await deleteLocally(model, primaryKey: primaryKey)
    }

    private func deleteLocally<T: BlackbirdSkybridgeSyncable>(_ type: T.Type, primaryKey: Blackbird.Value) async {
        guard let database else { return }
        try? await database.transaction { core in
            try Self.suppressingTriggers(core) {
                if let instance = try T.read(from: core, primaryKey: primaryKey) { try instance.delete(from: core) }
            }
        }
    }

    // MARK: - Account and zone resets

    private func handleAccountChange(_ event: CKSyncEngine.Event.AccountChange, engine: CKSyncEngine) async {
        if engine === sharedEngine {
            // Zones shared with the old account aren't shared with the new one.
            switch event.changeType {
                case .signOut, .switchAccounts:
                    for zone in zones.values where !zone.isOwn && zone.removedDate == nil {
                        await removeLocalZone(key: zone.key, reason: .accountChanged)
                    }
                default:
                    break
            }
            return
        }
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

    private func handleFetchedDatabaseChanges(_ changes: CKSyncEngine.Event.FetchedDatabaseChanges, engine: CKSyncEngine) async {
        for modification in changes.modifications {
            // A zone this device hasn't seen: shared with the user, or the user's own from another device.
            _ = await registeredZone(for: modification.zoneID)
        }
        for deletion in changes.deletions {
            let zoneID = deletion.zoneID
            guard let key = zoneKey(for: zoneID) else {
                // The default zone was deleted, purged, or reset on the server:
                // recreate it and re-upload everything in it.
                await scheduleFullUpload(clearingServerRecords: true, zoneKeys: [nil])
                continue
            }
            if !Self.isOwn(zoneID) {
                await removeLocalZone(key: key, reason: .shareRevoked)
                continue
            }
            switch deletion.reason {
                case .deleted:
                    // Another of the user's devices removed it (see removeZone).
                    await removeLocalZone(key: key, reason: .deletedElsewhere)
                case .purged, .encryptedDataReset:
                    await scheduleFullUpload(clearingServerRecords: true, zoneKeys: [key])
                @unknown default:
                    await scheduleFullUpload(clearingServerRecords: true, zoneKeys: [key])
            }
        }
    }

    /// Queues every row of every table in the user's own zones for upload, creating the zones;
    /// or just the rows of the zones given (nil among them for the default zone).
    private func scheduleFullUpload(clearingServerRecords: Bool, zoneKeys: Set<String?>? = nil) async {
        guard let engine = privateEngine else { return }
        var zoneIDs: [CKRecordZone.ID] = []
        if zoneKeys == nil || zoneKeys!.contains(nil) { zoneIDs.append(Self.defaultZoneID) }
        for zone in zones.values where zone.isOwn && zone.removedDate == nil && (zoneKeys == nil || zoneKeys!.contains(zone.key)) {
            zoneIDs.append(zone.zoneID)
        }
        engine.state.add(pendingDatabaseChanges: zoneIDs.map { .saveZone(CKRecordZone(zoneID: $0)) })
        for model in models.values {
            await scheduleFullUpload(model, clearingServerRecords: clearingServerRecords, zoneKeys: zoneKeys)
        }
        await topUpPendingChanges()
    }

    /// Queues the rows of a table for upload, in pages, without loading the table into memory:
    /// those in the user's own zones, or in the zones given. Rows in zones shared with the user
    /// aren't the user's to upload wholesale; their edits reconcile one by one.
    private func scheduleFullUpload<T: BlackbirdSkybridgeSyncable>(_ type: T.Type, clearingServerRecords: Bool, zoneKeys: Set<String?>?) async {
        guard let database else { return }
        let zoneColumn = T.skybridgeZoneColumnName()
        if zoneColumn == nil, let zoneKeys, !zoneKeys.contains(nil) { return }
        var lastRowID: Int64 = 0
        var queued = 0
        while true {
            let zoneSelect = zoneColumn.map { ", `\($0)` AS __zone" } ?? ""
            let rows = (try? await T.query(in: database, "SELECT rowid AS __rowid, `\(Self.primaryKeyName(T.self))` AS __pk, `\(skybridgeMetadataColumnName)` AS __meta\(zoneSelect) FROM `\(T.tableName)` WHERE rowid > ? ORDER BY rowid LIMIT \(Self.batchSize)", lastRowID)) ?? []
            guard !rows.isEmpty else {
                Self.logger.info("☁️ Skybridge full upload: \(queued) \(T.tableName, privacy: .public) rows queued")
                return
            }
            var keys: [(key: Blackbird.Value, zoneKey: String?)] = []
            var cleared: [(key: Blackbird.Value, metadata: SkybridgeMetadata)] = []
            for row in rows {
                guard let rowID = row["__rowid"]?.int64Value, let key = row["__pk"] else { continue }
                lastRowID = max(lastRowID, rowID)
                let zoneKey = zoneColumn == nil ? nil : T.skybridgeZoneKey(for: row["__zone"] ?? .null)
                if let zoneKeys, !zoneKeys.contains(zoneKey) { continue }
                if let zoneKey, let zone = zones[zoneKey], !zone.isOwn || zone.removedDate != nil { continue }
                keys.append((key, zoneKey))
                if clearingServerRecords, case .data(let data) = row["__meta"] ?? .null {
                    var metadata = SkybridgeMetadata.decode(data)
                    guard metadata.hasServerBase else { continue }
                    metadata.clearServerBase()
                    cleared.append((key, metadata))
                }
            }
            if !cleared.isEmpty {
                let batch = cleared
                try? await database.transaction { core in
                    try Self.suppressingTriggers(core) {
                        for (key, metadata) in batch { try Self.writeMetadata(metadata, for: T.self, primaryKey: key, in: core) }
                    }
                }
            }
            var changes: [CKSyncEngine.PendingRecordZoneChange] = []
            for (key, zoneKey) in keys {
                guard let keyString = key.stringValue, let zoneID = await zoneIDForUpload(key: zoneKey) else { continue }
                changes.append(.saveRecord(CKRecord.ID(recordName: recordName(tableName: T.tableName, primaryKeyString: keyString), zoneID: zoneID)))
            }
            queued += changes.count
            await enqueue(changes)
            await Task.yield()
        }
    }

    // MARK: - State persistence

    private func persistState(_ serialization: CKSyncEngine.State.Serialization, engine: CKSyncEngine) async {
        guard let database else { return }
        let data = try? JSONEncoder().encode(serialization)
        let id = engine === privateEngine ? BlackbirdSkybridgeState.privateEngineID : BlackbirdSkybridgeState.sharedEngineID
        try? await BlackbirdSkybridgeState(id: id, stateSerialization: data).write(to: database)
    }

    // MARK: - Record conversion

    private func recordForUpload<T: BlackbirdSkybridgeSyncable>(_ type: T.Type, primaryKey: Blackbird.Value, recordID: CKRecord.ID) async -> CKRecord? {
        guard let database else { return nil }
        // A missing last-write-wins timestamp (e.g. rows queued by a full upload) is
        // assigned and persisted here, before it's stamped into the uploaded record:
        // otherwise the post-save echo check would see a mismatch and re-upload every row.
        // Original-format metadata is upgraded on the way.
        let prepared: (row: Blackbird.Row, metadata: SkybridgeMetadata)? = (try? await database.transaction { core in
            guard let row = try Self.rawRow(T.self, primaryKey: primaryKey, in: core) else { return nil }
            var metadata = Self.metadata(of: row)
            var changed = Self.upgrade(&metadata, for: T.self)
            if metadata.syncLastModifiedDate == nil {
                metadata.syncLastModifiedDate = Date()
                changed = true
            }
            if changed { try Self.suppressingTriggers(core) { try Self.writeMetadata(metadata, for: T.self, primaryKey: primaryKey, in: core) } }
            return (row, metadata)
        }) ?? nil
        guard let prepared else { return nil }
        // A row whose zone differs from the one it was queued for stays out of that zone.
        let zoneKey = T.skybridgeZoneKey(of: prepared.row)
        guard zoneKey == self.zoneKey(for: recordID.zoneID) else { return nil }
        return Self.record(for: prepared.row, metadata: prepared.metadata, type: T.self, recordID: recordID)
    }

    /// The record a row uploads as: its raw column values, on the system fields of its server base.
    private static func record<T: BlackbirdSkybridgeSyncable>(for row: Blackbird.Row, metadata: SkybridgeMetadata, type: T.Type, recordID: CKRecord.ID) -> CKRecord {
        let archived = metadata.systemFieldsArchive(recordType: T.tableName).flatMap { decodeSystemFields($0) }
        // A base from another zone (a record moved between zone layouts) can't be built on.
        let record = archived.flatMap { $0.recordID.zoneID == recordID.zoneID ? $0 : nil } ?? CKRecord(recordType: T.tableName, recordID: recordID)
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

    // MARK: - Raw rows
    //
    // Synced columns are read and written as raw SQL values, never through the model type: a model
    // decodes only what it knows, and a value from a newer build (an enum case this one lacks, say)
    // comes out nil without error. Had that been written back, the server's value would be lost
    // locally, the row would read as a local edit, and the loss would upload to every device.
    //
    // Writes report their change by primary key and column, as a model write does, so the update
    // hook's row-ID-only report never becomes a whole-table change for the app or for Skybridge's
    // own change stream. Reads report nothing.

    private static func rawRow<T: BlackbirdSkybridgeSyncable>(_ type: T.Type, primaryKey: Blackbird.Value, in core: isolated Blackbird.Database.Core) throws -> Blackbird.Row? {
        try core.query("SELECT * FROM `\(T.tableName)` WHERE `\(primaryKeyName(T.self))` = ?", primaryKey).first
    }

    private static func rawRows<T: BlackbirdSkybridgeSyncable>(_ type: T.Type, primaryKeys: [Blackbird.Value], in database: Blackbird.Database) async throws -> [Blackbird.Row] {
        guard !primaryKeys.isEmpty else { return [] }
        let placeholders = Array(repeating: "?", count: primaryKeys.count).joined(separator: ",")
        return try await database.query("SELECT * FROM `\(T.tableName)` WHERE `\(primaryKeyName(T.self))` IN (\(placeholders))", arguments: primaryKeys)
    }

    private static func metadata(of row: Blackbird.Row) -> SkybridgeMetadata {
        if case .data(let data) = row[skybridgeMetadataColumnName] ?? .null { return SkybridgeMetadata.decode(data) }
        return SkybridgeMetadata()
    }

    private static func value(of metadata: SkybridgeMetadata) -> Blackbird.Value {
        metadata.encoded().map { .data($0) } ?? .null
    }

    private static func writeMetadata<T: BlackbirdSkybridgeSyncable>(_ metadata: SkybridgeMetadata, for type: T.Type, primaryKey: Blackbird.Value, in core: isolated Blackbird.Database.Core) throws {
        try update(T.self, primaryKey: primaryKey, columns: [(skybridgeMetadataColumnName, value(of: metadata))], in: core)
    }

    /// Sets columns of an existing row.
    internal static func update<T: BlackbirdSkybridgeSyncable>(_ type: T.Type, primaryKey: Blackbird.Value, columns: [(name: String, value: Blackbird.Value)], in core: isolated Blackbird.Database.Core) throws {
        guard !columns.isEmpty else { return }
        let assignments = columns.map { "`\($0.name)` = ?" }.joined(separator: ", ")
        try write(T.self, primaryKey: primaryKey, columns: Set(columns.map(\.name)), in: core,
                  "UPDATE `\(T.tableName)` SET \(assignments) WHERE `\(primaryKeyName(T.self))` = ?", arguments: columns.map(\.value) + [primaryKey])
    }

    /// Adds a row; columns not given take the table's defaults.
    internal static func insert<T: BlackbirdSkybridgeSyncable>(_ type: T.Type, primaryKey: Blackbird.Value, columns: [(name: String, value: Blackbird.Value)], in core: isolated Blackbird.Database.Core) throws {
        let names = [primaryKeyName(T.self)] + columns.map(\.name)
        let placeholders = Array(repeating: "?", count: names.count).joined(separator: ",")
        try write(T.self, primaryKey: primaryKey, columns: Set(names), in: core,
                  "INSERT INTO `\(T.tableName)` (`\(names.joined(separator: "`,`"))`) VALUES (\(placeholders))", arguments: [primaryKey] + columns.map(\.value))
    }

    /// Runs a statement that changes one row, reporting the change by key as `BlackbirdModel.write` does.
    private static func write<T: BlackbirdSkybridgeSyncable>(_ type: T.Type, primaryKey: Blackbird.Value, columns: Blackbird.ColumnNames, in core: isolated Blackbird.Database.Core, _ sql: String, arguments: [Blackbird.Value]) throws {
        let reporter = try core.database().changeReporter
        reporter.ignoreWritesToTable(T.tableName)
        do {
            try core.query(sql, arguments: arguments)
        } catch {
            reporter.stopIgnoringWrites()
            throw error
        }
        reporter.stopIgnoringWrites()
        reporter.reportChange(tableName: T.tableName, primaryKeys: [[primaryKey]], changedColumns: columns)
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
        localFingerprint(of: rowRepresentation(of: instance), for: T.self, metadata: metadata)
    }

    internal static func localFingerprint<T: BlackbirdSkybridgeSyncable>(of row: Blackbird.Row, for type: T.Type, metadata: SkybridgeMetadata) -> Data {
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

    /// Every column of an instance as a raw row, keyed by column name. For tests; Skybridge itself
    /// reads rows raw from the database.
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
                await persistState(stateUpdate.stateSerialization, engine: syncEngine)
            case .accountChange(let accountChange):
                await handleAccountChange(accountChange, engine: syncEngine)
            case .fetchedDatabaseChanges(let changes):
                await handleFetchedDatabaseChanges(changes, engine: syncEngine)
            case .fetchedRecordZoneChanges(let changes):
                await handleFetchedRecordZoneChanges(changes)
            case .sentRecordZoneChanges(let sent):
                await handleSentRecordZoneChanges(sent, engine: syncEngine)
            case .sentDatabaseChanges(let sent):
                for (zoneID, error) in sent.failedZoneDeletes where error.code != .zoneNotFound {
                    Self.logger.error("☁️🛑 Skybridge: couldn't delete zone \(zoneID.zoneName, privacy: .public): \(error, privacy: .public)")
                }
                for failure in sent.failedZoneSaves {
                    Self.logger.error("☁️🛑 Skybridge: couldn't create zone \(failure.zone.zoneID.zoneName, privacy: .public): \(failure.error, privacy: .public)")
                }
            case .didSendChanges:
                await topUpPendingChanges(engineID: syncEngine === privateEngine ? BlackbirdSkybridgeState.privateEngineID : BlackbirdSkybridgeState.sharedEngineID)
            case .willFetchChanges:
                await noteFetching(true, engine: syncEngine)
            case .didFetchChanges:
                // Whatever zones it had in progress are done with it.
                fetchingZones.removeAll()
                await noteFetching(false, engine: syncEngine)
            case .willFetchRecordZoneChanges(let event):
                await noteFetching(true, engine: syncEngine, zoneID: event.zoneID)
            case .didFetchRecordZoneChanges(let event):
                await noteFetching(false, engine: syncEngine, zoneID: event.zoneID)
            case .willSendChanges:
                break
            @unknown default:
                break
        }
    }

    public func nextRecordZoneChangeBatch(_ context: CKSyncEngine.SendChangesContext, syncEngine: CKSyncEngine) async -> CKSyncEngine.RecordZoneChangeBatch? {
        let scope = context.options.scope
        let pendingChanges = syncEngine.state.pendingRecordZoneChanges.filter { scope.contains($0) }
        guard !pendingChanges.isEmpty else { return nil }
        for change in pendingChanges {
            if case .deleteRecord(let id) = change { inFlightDeletions.insert(id.recordName) }
        }

        return await CKSyncEngine.RecordZoneChangeBatch(pendingChanges: pendingChanges) { recordID in
            guard let (model, primaryKey) = await self.modelAndPrimaryKey(for: recordID),
                  let record = await self.recordForUpload(model, primaryKey: primaryKey, recordID: recordID)
            else {
                // Unknown table, deleted locally since it was queued, or no longer in this zone.
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
