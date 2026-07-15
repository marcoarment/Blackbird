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
@preconcurrency import Combine
import os.log

/// A BlackbirdModel type that ``BlackbirdSkybridge`` can sync to CloudKit.
///
/// Conforming types must declare these two bookkeeping columns, which Skybridge manages:
///
///     @BlackbirdColumn var syncLastModifiedDate: Date?
///     @BlackbirdColumn var ckRecordData: Data?
///
/// Syncing is opt-in per column: only the columns listed in ``skybridgeSyncedColumns``
/// are sent to CloudKit.
///
/// Requirements: an explicitly declared, single-column, non-Data `primaryKey`
/// (which becomes the CloudKit record name), and column names that are valid
/// CloudKit field names. Columns not listed in ``skybridgeSyncedColumns`` should
/// be optional or have sensible defaults, since rows created from server records
/// decode them as `NULL`.
public protocol BlackbirdSkybridgeSyncable: BlackbirdModel {
    /// An archive of the CKRecord as last synced with the server. Managed by Skybridge.
    var ckRecordData: Data? { get set }

    /// The local-edit timestamp used for last-write-wins conflict resolution.
    /// Skybridge sets it automatically when it detects a local change, but writers
    /// may set it themselves for more-precise conflict timestamps.
    var syncLastModifiedDate: Date? { get set }

    /// Columns that should be sent to CloudKit, e.g. `[ \.$title, \.$dueDate ]`.
    static var skybridgeSyncedColumns: [BlackbirdColumnKeyPath] { get }
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

    fileprivate static func skybridgeSyncedColumnInfo() -> [(keyPath: PartialKeyPath<Self>, name: String)] {
        skybridgeSyncedColumns.map { ($0, columnInfoFromKeyPath($0).name) }
    }

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
        _ = skybridgeSyncedColumnInfo() // traps on key-paths that aren't @BlackbirdColumn columns
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

/// Syncs ``BlackbirdSkybridgeSyncable`` tables to CloudKit via CKSyncEngine.
///
/// Sync is fully automatic and silent: conflicts resolve to the latest write,
/// and errors are logged and retried without ever involving the user.
///
/// Each row's `ckRecordData` holds an archive of the record as last synced with
/// the server. Comparing a row against that archive distinguishes genuine local
/// edits (which get uploaded) from echoes of sync-applied writes (ignored), so
/// no in-memory bookkeeping is needed and devices can't ping-pong updates.
///
/// All rows sync as records in a single private "Skybridge" zone, with the record
/// type set to the table name and the record name set to "tableName:primaryKey".
@available(macOS 14, iOS 17, tvOS 17, watchOS 10, *)
public actor BlackbirdSkybridge {
    private enum Field {
        static let lastModified = "skybridge_lastModified"
    }

    // The protocol's bookkeeping properties, as column names. These can't be
    // derived from key-paths because a protocol can't require the `$`-prefixed
    // column wrappers, only the wrapped properties.
    private enum Column {
        static let ckRecordData = "ckRecordData"
        static let syncLastModifiedDate = "syncLastModifiedDate"
    }

    private static let logger = Logger(subsystem: Blackbird.loggingSubsystem, category: "Skybridge")

    private let zoneID = CKRecordZone.ID(zoneName: "Skybridge")
    private let syncedTypes: [any BlackbirdSkybridgeSyncable.Type]

    private var models: [String: any BlackbirdSkybridgeSyncable.Type] = [:] // keyed by tableName == CKRecord type
    private var database: Blackbird.Database?
    private var engine: CKSyncEngine?
    private var localChangeTasks: [Task<Void, Never>] = []

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
            // Catch local changes made before this subscription existed (e.g. rows
            // written between database open and start, or by another process).
            for type in syncedTypes {
                await handleLocalChange(tableName: type.tableName, primaryKeys: nil)
            }
        }
    }

    // MARK: - Record identity

    private func recordID(tableName: String, primaryKeyString: String) -> CKRecord.ID {
        CKRecord.ID(recordName: "\(tableName):\(primaryKeyString)", zoneID: zoneID)
    }

    /// Resolves a record ID back to its model type and primary-key value. Record
    /// names are "tableName:primaryKey"; table names never contain a colon.
    private func modelAndPrimaryKey(for recordID: CKRecord.ID) -> (model: any BlackbirdSkybridgeSyncable.Type, primaryKey: Blackbird.Value)? {
        let name = recordID.recordName
        guard let separator = name.firstIndex(of: ":"), let model = models[String(name[name.startIndex..<separator])] else { return nil }
        let primaryKeyString = String(name[name.index(after: separator)...])
        guard let primaryKey = model.skybridgePrimaryKeyValue(fromString: primaryKeyString) else { return nil }
        return (model, primaryKey)
    }

    // MARK: - Local changes

    private func handleLocalChange(tableName: String, primaryKeys: Blackbird.PrimaryKeyValues?) async {
        guard let model = models[tableName] else { return }

        if let primaryKeys {
            for key in primaryKeys {
                guard let primaryKey = key.first else { continue }
                await reconcileLocalRow(model, primaryKey: primaryKey)
            }
        } else {
            // Unknown scope of changes: re-check every row.
            await reconcileAllRows(model)
        }
    }

    private func reconcileAllRows<T: BlackbirdSkybridgeSyncable>(_ type: T.Type) async {
        guard let database else { return }
        for instance in (try? await T.read(from: database, matching: .all)) ?? [] {
            await reconcileLocalRow(type, primaryKey: Self.primaryKeyValue(of: instance))
        }
    }

    private enum ReconcileAction: Sendable {
        case ignore, upload, deleteFromServer
    }

    /// Compares a row against its last-synced record and schedules an upload
    /// (or a deletion) if they differ. Rows that match their last-synced state
    /// are echoes of changes this engine just applied, and are ignored.
    private func reconcileLocalRow<T: BlackbirdSkybridgeSyncable>(_ type: T.Type, primaryKey: Blackbird.Value) async {
        guard let database, let engine, let primaryKeyString = primaryKey.stringValue else { return }
        let recordID = recordID(tableName: T.tableName, primaryKeyString: primaryKeyString)

        // The read, comparison, and timestamp bump happen in one transaction so a
        // concurrent writer can't be overwritten by a stale full-row rewrite.
        let action: ReconcileAction = (try? await database.transaction { core in
            guard var instance = try T.read(from: core, primaryKey: primaryKey) else { return ReconcileAction.deleteFromServer }

            let lastSyncedRecord = instance.ckRecordData.flatMap { Self.decodeRecord($0) }
            if let lastSyncedRecord, Self.recordMatches(lastSyncedRecord, instance: instance) { return ReconcileAction.ignore }

            // A genuine local edit: bump the last-write-wins timestamp if the
            // writer didn't. (The rewrite re-enters here once, then no-ops.)
            let lastSyncedModified = lastSyncedRecord?.encryptedValues[Field.lastModified] as? Date
            let localModified = instance.syncLastModifiedDate
            if localModified == nil || (lastSyncedModified != nil && localModified! <= lastSyncedModified!) {
                instance.syncLastModifiedDate = Date()
                try instance.write(to: core)
            }
            return ReconcileAction.upload
        }) ?? .ignore // a failed read/write here must not become a server deletion

        switch action {
            case .ignore:
                break
            case .upload:
                engine.state.add(pendingRecordZoneChanges: [.saveRecord(recordID)])
            case .deleteFromServer:
                if !consumeServerDeletion(recordName: recordID.recordName) {
                    engine.state.add(pendingRecordZoneChanges: [.deleteRecord(recordID)])
                }
        }
    }

    // MARK: - Server changes

    /// Applies a record fetched from (or rejected by) the server, keeping the
    /// latest write: if the local row was modified more recently, its values
    /// win and are re-uploaded on top of the server record; otherwise the
    /// server values are applied locally.
    private func applyServerRecord<T: BlackbirdSkybridgeSyncable>(_ type: T.Type, record: CKRecord, primaryKey: Blackbird.Value) async {
        guard let database, let engine else { return }
        let serverModified = record.encryptedValues[Field.lastModified] as? Date ?? record.modificationDate ?? .distantPast
        guard let archive = Self.encodeRecord(record) else { return }
        let recordID = record.recordID

        // Extracted before the transaction: CKRecord isn't Sendable.
        let serverValues: [(name: String, value: Blackbird.Value)] = T.skybridgeSyncedColumnInfo().map {
            ($0.name, Self.blackbirdValue(from: record.encryptedValues[$0.name]))
        }

        do {
            // One transaction, so a concurrent local writer can't be clobbered by a stale row.
            let localWins: Bool = try await database.transaction { core in
                let existing = try T.read(from: core, primaryKey: primaryKey)

                if var existing, let localModified = existing.syncLastModifiedDate, localModified > serverModified {
                    // Local wins: keep local values, adopt the server record as the new base, re-send.
                    existing.ckRecordData = archive
                    try existing.write(to: core)
                    return true
                }

                // Server wins (or no local row): overlay the server values onto the row and write it back.
                var row: Blackbird.Row = existing.map { Self.rowRepresentation(of: $0) } ?? [:]
                row[T.skybridgePrimaryKeyInfo().name] = primaryKey
                for (name, value) in serverValues { row[name] = value }
                row[Column.syncLastModifiedDate] = .double(serverModified.timeIntervalSince1970)
                row[Column.ckRecordData] = .data(archive)

                let updated = try T.instance(from: row, in: database)
                try updated.write(to: core)
                return false
            }
            if localWins { engine.state.add(pendingRecordZoneChanges: [.saveRecord(recordID)]) }
        } catch {
            Self.logger.error("☁️🛑 Skybridge: cannot apply server record \(recordID.recordName, privacy: .public): \(error, privacy: .public)")
        }
    }

    private func applyServerDeletion<T: BlackbirdSkybridgeSyncable>(_ type: T.Type, recordID: CKRecord.ID, primaryKey: Blackbird.Value) async {
        guard let database, let instance = try? await T.read(from: database, primaryKey: primaryKey) else { return }
        noteServerDeletion(recordName: recordID.recordName)
        try? await instance.delete(from: database)
    }

    /// Replaces a row's last-synced record archive, or discards it to force a fresh upload.
    /// - Returns: Whether the row exists.
    @discardableResult
    private func storeServerArchive<T: BlackbirdSkybridgeSyncable>(_ type: T.Type, primaryKey: Blackbird.Value, archive: Data?) async -> Bool {
        guard let database else { return false }
        do {
            try await T.modify(in: database, primaryKey: primaryKey) { _, instance in
                instance.ckRecordData = archive
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
            await storeServerArchive(model, primaryKey: primaryKey, archive: Self.encodeRecord(record))
        }

        for failedSave in event.failedRecordSaves {
            let recordID = failedSave.record.recordID
            switch failedSave.error.code {
                case .serverRecordChanged:
                    if let serverRecord = failedSave.error.serverRecord, let (model, primaryKey) = modelAndPrimaryKey(for: recordID) {
                        await applyServerRecord(model, record: serverRecord, primaryKey: primaryKey)
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
    }

    /// Discards stale server metadata for a row and schedules a fresh upload.
    private func clearServerRecordAndResend(recordID: CKRecord.ID) async {
        guard let engine, let (model, primaryKey) = modelAndPrimaryKey(for: recordID) else { return }
        if await storeServerArchive(model, primaryKey: primaryKey, archive: nil) {
            engine.state.add(pendingRecordZoneChanges: [.saveRecord(recordID)])
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
    }

    private func scheduleFullUpload<T: BlackbirdSkybridgeSyncable>(_ type: T.Type, clearingServerRecords: Bool) async {
        guard let database, let engine else { return }
        for instance in (try? await T.read(from: database, matching: .all)) ?? [] {
            let primaryKey = Self.primaryKeyValue(of: instance)
            guard let primaryKeyString = primaryKey.stringValue else { continue }
            if clearingServerRecords, instance.ckRecordData != nil {
                try? await T.modify(in: database, primaryKey: primaryKey) { _, instance in
                    instance.ckRecordData = nil
                }
            }
            engine.state.add(pendingRecordZoneChanges: [.saveRecord(recordID(tableName: T.tableName, primaryKeyString: primaryKeyString))])
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
        let instance: T? = try? await T.modify(in: database, primaryKey: primaryKey) { _, instance in
            if instance.syncLastModifiedDate == nil { instance.syncLastModifiedDate = Date() }
            return instance
        }
        guard let instance else { return nil }
        return Self.record(for: instance, recordID: recordID)
    }

    private static func record<T: BlackbirdSkybridgeSyncable>(for instance: T, recordID: CKRecord.ID) -> CKRecord {
        let record = instance.ckRecordData.flatMap { decodeRecord($0) } ?? CKRecord(recordType: T.tableName, recordID: recordID)
        for (keyPath, name) in T.skybridgeSyncedColumnInfo() {
            setValue(columnValue(of: instance, at: keyPath), on: record, forKey: name)
        }
        record.encryptedValues[Field.lastModified] = instance.syncLastModifiedDate ?? Date()
        return record
    }

    private static func recordMatches<T: BlackbirdSkybridgeSyncable>(_ record: CKRecord, instance: T) -> Bool {
        for (keyPath, name) in T.skybridgeSyncedColumnInfo() {
            if !valuesEquivalent(columnValue(of: instance, at: keyPath), blackbirdValue(from: record.encryptedValues[name])) { return false }
        }
        // Falls back to modificationDate like applyServerRecord does, so a record from a
        // client that doesn't set the field can't re-upload once per receiving device.
        return datesEquivalent(instance.syncLastModifiedDate, (record.encryptedValues[Field.lastModified] as? Date) ?? record.modificationDate)
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

    /// Equality with cross-type numeric tolerance, since SQLite column affinity
    /// may store an uploaded integer as a double or vice versa.
    private static func valuesEquivalent(_ a: Blackbird.Value, _ b: Blackbird.Value) -> Bool {
        if a == b { return true }
        switch (a, b) {
            case (.integer(let i), .double(let d)), (.double(let d), .integer(let i)): return Double(i) == d
            default: return false
        }
    }

    private static func datesEquivalent(_ a: Date?, _ b: Date?) -> Bool {
        switch (a, b) {
            case (nil, nil): true
            case (let a?, let b?): abs(a.timeIntervalSince(b)) < 0.001
            default: false
        }
    }

    private static func encodeRecord(_ record: CKRecord) -> Data? {
        try? NSKeyedArchiver.archivedData(withRootObject: record, requiringSecureCoding: true)
    }

    private static func decodeRecord(_ data: Data) -> CKRecord? {
        try? NSKeyedUnarchiver.unarchivedObject(ofClass: CKRecord.self, from: data)
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
                for modification in changes.modifications {
                    guard let (model, primaryKey) = modelAndPrimaryKey(for: modification.record.recordID) else { continue }
                    await applyServerRecord(model, record: modification.record, primaryKey: primaryKey)
                }
                for deletion in changes.deletions {
                    guard let (model, primaryKey) = modelAndPrimaryKey(for: deletion.recordID) else { continue }
                    await applyServerDeletion(model, recordID: deletion.recordID, primaryKey: primaryKey)
                }
            case .sentRecordZoneChanges(let sent):
                await handleSentRecordZoneChanges(sent)
            case .sentDatabaseChanges, .willFetchChanges, .willFetchRecordZoneChanges, .didFetchRecordZoneChanges, .didFetchChanges, .willSendChanges, .didSendChanges:
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

#endif // canImport(CloudKit)
