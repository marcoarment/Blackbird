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
//  BlackbirdChanges.swift
//  Created by Marco Arment on 11/17/22.
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

import Foundation
@preconcurrency import Combine

public extension Blackbird {
    /// A change to a table in a Blackbird database, as published by a ``ChangePublisher``.
    ///
    /// For `BlackbirdModel` tables, ``BlackbirdModel/changePublisher(in:)`` provides a typed ``ModelChange`` instead.
    struct Change: Sendable {
        internal let table: String
        internal let primaryKeys: PrimaryKeyValues?
        internal let columnNames: Blackbird.ColumnNames?
        
        /// Determine if a specific primary-key value may have changed.
        /// - Parameter key: The single-column primary-key value in question.
        /// - Returns: Whether the row with this primary-key value may have changed. Note that changes may be over-reported.
        ///
        /// For tables with multi-column primary keys, use ``hasMulticolumnPrimaryKeyChanged(_:)``.
        public func hasPrimaryKeyChanged(_ key: Any) -> Bool {
            guard let primaryKeys else { return true }
            return primaryKeys.contains([try! Blackbird.Value.fromAny(key)])
        }
        
        /// Determine if a specific primary-key value may have changed in a table with a multi-column primary key.
        /// - Parameter key: The multi-column primary-key value array in question.
        /// - Returns: Whether the row with these primary-key values may have changed. Note that changes may be over-reported.
        ///
        /// For tables with single-column primary keys, use ``hasPrimaryKeyChanged(_:)``.
        public func hasMulticolumnPrimaryKeyChanged(_ key: [Any]) -> Bool {
            guard let primaryKeys else { return true }
            return primaryKeys.contains(key.map { try! Blackbird.Value.fromAny($0) })
        }
        
        /// Determine if a specific column may have changed.
        /// - Parameter columnName: The column name.
        /// - Returns: Whether this column may have changed in any rows. Note that changes may be over-reported.
        public func hasColumnChanged(_ columnName: String) -> Bool {
            guard let columnNames else { return true }
            return columnNames.contains(columnName)
        }
    }

    /// A Publisher that emits when data in a Blackbird table has changed.
    ///
    /// The ``Blackbird/Change`` passed indicates which rows and columns in the table have changed.
    typealias ChangePublisher = AnyPublisher<Change, Never>

    /// A change to a table in a Blackbird database, as published by a ``ChangePublisher``.
    struct ModelChange<T: BlackbirdModel>: Sendable {
        internal let type: T.Type
        internal let primaryKeys: PrimaryKeyValues?
        internal let columnNames: Blackbird.ColumnNames?

        /// Determine if a specific primary-key value may have changed.
        /// - Parameter key: The single-column primary-key value in question.
        /// - Returns: Whether the row with this primary-key value may have changed. Note that changes may be over-reported.
        ///
        /// For tables with multi-column primary keys, use ``hasMulticolumnPrimaryKeyChanged(_:)``.
        public func hasPrimaryKeyChanged(_ key: Any) -> Bool {
            guard let primaryKeys else { return true }
            return primaryKeys.contains([try! Blackbird.Value.fromAny(key)])
        }
        
        /// Determine if a specific primary-key value may have changed in a table with a multi-column primary key.
        /// - Parameter key: The multi-column primary-key value array in question.
        /// - Returns: Whether the row with these primary-key values may have changed. Note that changes may be over-reported.
        ///
        /// For tables with single-column primary keys, use ``hasPrimaryKeyChanged(_:)``.
        public func hasMulticolumnPrimaryKeyChanged(_ key: [Any]) -> Bool {
            guard let primaryKeys else { return true }
            return primaryKeys.contains(key.map { try! Blackbird.Value.fromAny($0) })
        }
        
        /// Determine if a specific column name may have changed.
        /// - Parameter columnName: The column name.
        /// - Returns: Whether this column may have changed in any rows. Note that changes may be over-reported.
        public func hasColumnChanged(_ columnName: String) -> Bool {
            guard let columnNames else { return true }
            return columnNames.contains(columnName)
        }

        /// Determine if a specific column key-path may have changed.
        /// - Parameter keyPath: The column key-path using its `$`-prefixed wrapper, e.g. `\.$title`.
        /// - Returns: Whether this column may have changed in any rows. Note that changes may be over-reported.
        public func hasColumnChanged(_ keyPath: T.BlackbirdColumnKeyPath) -> Bool {
            guard let columnNames else { return true }
            return columnNames.contains(T.table.keyPathToColumnName(keyPath: keyPath))
        }

        internal init(type: T.Type, from change: Change) {
            self.type = type
            self.primaryKeys = change.primaryKeys
            self.columnNames = change.columnNames
        }
    }

    /// A Publisher that emits when data in a BlackbirdModel table has changed.
    ///
    /// The ``Blackbird/ModelChange`` passed indicates which rows and columns in the table have changed.
    typealias ModelChangePublisher<T: BlackbirdModel> = AnyPublisher<ModelChange<T>, Never>

    internal static func isRelevantPrimaryKeyChange(watchedPrimaryKeys: Blackbird.PrimaryKeyValues?, changedPrimaryKeys: Blackbird.PrimaryKeyValues?) -> Bool {
        guard let watchedPrimaryKeys else {
            // Not watching any particular keys -- always update for any table change
            return true
        }
        
        guard let changedPrimaryKeys else {
            // Change sent for unknown/all keys -- always update
            return true
        }
        
        if !watchedPrimaryKeys.isDisjoint(with: changedPrimaryKeys) {
            // Overlapping keys -- update
            return true
        }
        
        return false
    }
}

// MARK: - Change publisher

extension Blackbird.Database {

    /// The ``Blackbird/ChangePublisher`` for the specified table.
    /// - Parameter tableName: The table name.
    /// - Returns: A ``Blackbird/ChangePublisher`` that publishes ``Blackbird/Change`` objects for each change in the specified table.
    ///
    /// For `BlackbirdModel` tables, ``BlackbirdModel/changePublisher(in:)`` provides a typed ``Blackbird/ModelChange`` instead.
    ///
    /// > - The publisher may send from any thread.
    /// > - Changes may be over-reported.
    public func changePublisher(for tableName: String) -> Blackbird.ChangePublisher { changeReporter.changePublisher(for: tableName) }

    internal final class ChangeReporter: @unchecked Sendable /* unchecked due to use of internal locking */ {
        internal final class AccumulatedChanges {
            var primaryKeys: Blackbird.PrimaryKeyValues? = Blackbird.PrimaryKeyValues()
            var columnNames: Blackbird.ColumnNames? = Blackbird.ColumnNames()
            static func entireTableChange(columnsIfKnown: Blackbird.ColumnNames? = nil) -> Self {
                let s = Self.init()
                s.primaryKeys = nil
                s.columnNames = columnsIfKnown
                return s
            }
        }
    
        private let lock = Blackbird.Lock()
        private var activeTransactions = Set<Int64>()
        private var ignoreWritesToTableName: String? = nil
        private var bufferRowIDsForIgnoredTable = false
        private var bufferedRowIDsForIgnoredTable = Set<Int64>()
        
        private var accumulatedChangesByTable: [String: AccumulatedChanges] = [:]
        private var tableChangePublishers: [String: PassthroughSubject<Blackbird.Change, Never>] = [:]
        
        private var debugPrintEveryReportedChange = false
        
        private var cache: Blackbird.Database.Cache
        
        internal var numChangesReportedByUpdateHook: UInt64 = 0
        
        init(options: Options, cache: Blackbird.Database.Cache) {
            debugPrintEveryReportedChange = options.contains(.debugPrintEveryReportedChange)
            self.cache = cache
        }

        internal func changePublisher(for tableName: String) -> Blackbird.ChangePublisher {
            lock.withLock {
                if let existing = tableChangePublishers[tableName] { return existing.eraseToAnyPublisher() }
                let publisher = PassthroughSubject<Blackbird.Change, Never>()
                tableChangePublishers[tableName] = publisher
                return publisher.eraseToAnyPublisher()
            }
        }

        internal func ignoreWritesToTable(_ name: String, beginBufferingRowIDs: Bool = false) {
            lock.lock()
            ignoreWritesToTableName = name
            bufferRowIDsForIgnoredTable = beginBufferingRowIDs
            bufferedRowIDsForIgnoredTable.removeAll()
            lock.unlock()
        }

        @discardableResult
        internal func stopIgnoringWrites() -> Set<Int64> {
            lock.lock()
            ignoreWritesToTableName = nil
            bufferRowIDsForIgnoredTable = false
            let rowIDs = bufferedRowIDsForIgnoredTable
            bufferedRowIDsForIgnoredTable.removeAll()
            lock.unlock()
            return rowIDs
        }

        internal func beginTransaction(_ transactionID: Int64) {
            lock.lock()
            activeTransactions.insert(transactionID)
            lock.unlock()
        }

        internal func endTransaction(_ transactionID: Int64) {
            lock.lock()
            activeTransactions.remove(transactionID)
            let needsFlush = activeTransactions.isEmpty && !accumulatedChangesByTable.isEmpty
            lock.unlock()
            if needsFlush { flush() }
        }
        
        internal func reportEntireDatabaseChange() {
            if debugPrintEveryReportedChange { print("[Blackbird.ChangeReporter] ⚠️ database changed externally, reporting changes to all tables!") }
            
            cache.invalidate()

            lock.lock()
            for tableName in tableChangePublishers.keys { accumulatedChangesByTable[tableName] = AccumulatedChanges.entireTableChange() }
            let needsFlush = activeTransactions.isEmpty
            lock.unlock()
            if needsFlush { flush() }
        }

        internal func reportChange(tableName: String, primaryKeys: [[Blackbird.Value]]? = nil, rowID: Int64? = nil, changedColumns: Blackbird.ColumnNames?) {
            lock.lock()
            let needsFlush: Bool
            if tableName == ignoreWritesToTableName {
                if let rowID, bufferRowIDsForIgnoredTable { bufferedRowIDsForIgnoredTable.insert(rowID) }
                needsFlush = false
            } else {
                if let primaryKeys, !primaryKeys.isEmpty {
                    if accumulatedChangesByTable[tableName] == nil { accumulatedChangesByTable[tableName] = AccumulatedChanges() }
                    accumulatedChangesByTable[tableName]!.primaryKeys?.formUnion(primaryKeys)
                    
                    if let changedColumns {
                        accumulatedChangesByTable[tableName]!.columnNames?.formUnion(changedColumns)
                    } else {
                        accumulatedChangesByTable[tableName]!.columnNames = nil
                    }
                    
                    for primaryKey in primaryKeys {
                        if primaryKey.count == 1 { cache.invalidate(tableName: tableName, primaryKeyValue: primaryKey.first) }
                        else { cache.invalidate(tableName: tableName) }
                    }
                } else {
                    accumulatedChangesByTable[tableName] = AccumulatedChanges.entireTableChange(columnsIfKnown: changedColumns)
                    cache.invalidate(tableName: tableName)
                }

                needsFlush = activeTransactions.isEmpty
            }
            lock.unlock()
            if needsFlush { flush() }
        }
        
        private func flush() {
            lock.lock()
            let publishers = tableChangePublishers
            let changesByTable = accumulatedChangesByTable
            accumulatedChangesByTable.removeAll()
            lock.unlock()
            
            for (tableName, accumulatedChanges) in changesByTable {
                if let keys = accumulatedChanges.primaryKeys {
                    if debugPrintEveryReportedChange {
                        print("[Blackbird.ChangeReporter] changed \(tableName) (\(keys.count) keys, fields: \(accumulatedChanges.columnNames?.joined(separator: ",") ?? "(all/unknown)"))")
                    }
                    if let publisher = publishers[tableName] { publisher.send(Blackbird.Change(table: tableName, primaryKeys: keys, columnNames: accumulatedChanges.columnNames)) }
                } else {
                    if debugPrintEveryReportedChange { print("[Blackbird.ChangeReporter] changed \(tableName) (unknown keys, fields: \(accumulatedChanges.columnNames?.joined(separator: ",") ?? "(all/unknown)"))") }
                    if let publisher = publishers[tableName] { publisher.send(Blackbird.Change(table: tableName, primaryKeys: nil, columnNames: accumulatedChanges.columnNames)) }
                }
            }
        }        
    }
}

// MARK: - General query cache with Combine publisher

extension Blackbird {

    /// A function to generate arbitrary results from a database, called from an async throwing context and passed the ``Blackbird/Database`` as its sole argument.
    ///
    /// Used by Blackbird's SwiftUI property wrappers.
    ///
    /// ## Examples
    ///
    /// ```swift
    /// { try await Post.read(from: $0, id: 123) }
    /// ```
    /// ```swift
    /// { try await $0.query("SELECT COUNT(*) FROM Post") }
    /// ```
    public typealias CachedResultGenerator<T: Sendable> = (@Sendable (_ db: Blackbird.Database) async throws -> T)

    internal final class CachedResultPublisher<T: Sendable>: Sendable {
        public let valuePublisher: CurrentValueSubject<T?, Never>

        private struct State: Sendable {
            fileprivate var cachedResults: T? = nil
            fileprivate var tableName: String? = nil
            fileprivate var database: Blackbird.Database? = nil
            fileprivate var generator: CachedResultGenerator<T>? = nil
            fileprivate var tableChangePublisher: AnyCancellable? = nil
        }
        
        private let config = Locked(State())
        
        public init(initialValue: T? = nil) {
            valuePublisher = CurrentValueSubject<T?, Never>(initialValue)
        }

        public func subscribe(to tableName: String, in database: Blackbird.Database?, generator: CachedResultGenerator<T>?) {
            config.withLock {
                $0.tableName = tableName
                $0.generator = generator
            }
            self.changeDatabase(database)
            enqueueUpdate()
        }
        
        private func update(_ cachedResults: T?) async throws {
            let state = config.value
            let results: T?
            if let cachedResults = state.cachedResults {
                results = cachedResults
            } else {
                results = (state.generator != nil && state.database != nil ? try await state.generator!(state.database!) : nil)
                config.withLock { $0.cachedResults = results }
                valuePublisher.send(results)
            }
        }
        
        private func changeDatabase(_ newDatabase: Database?) {
            config.withLock {
                if newDatabase == $0.database { return }
                
                $0.database = newDatabase
                $0.cachedResults = nil

                if let database = $0.database, let tableName = $0.tableName {
                    $0.tableChangePublisher = database.changeReporter.changePublisher(for: tableName).sink { [weak self] _ in
                        guard let self else { return }
                        self.config.withLock { $0.cachedResults = nil }
                        self.enqueueUpdate()
                    }
                } else {
                    $0.tableChangePublisher = nil
                }
            }
        }
        
        private func enqueueUpdate() {
            let cachedResults = config.withLock { $0.cachedResults }
            Task.detached { [weak self] in
                do { try await self?.update(cachedResults) }
                catch { print("[Blackbird.CachedResultPublisher<\(String(describing: T.self))>] ⚠️ Error updating: \(error.localizedDescription)") }
            }
        }
    }
}
