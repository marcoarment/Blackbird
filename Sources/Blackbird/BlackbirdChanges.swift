//
//  BlackbirdChanges.swift
//  Created by Marco Arment on 11/17/22.
//  Copyright (c) 2022 Marco Arment
//
//  Released under the MIT License
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
import Combine

public extension Blackbird {
    /// A Publisher that emits when data in a Blackbird table has changed.
    ///
    /// The ``PrimaryKeyValues`` value passed indicates which rows in the table have changed:
    /// * If the value is non-`nil`, only the rows with the given primary-key values may have changed.
    /// * If the value is `nil`, any rows in the table may have changed.
    ///
    /// ## Example
    /// ```swift
    /// let db = try Blackbird.Database.inMemoryDatabase()
    /// // ...
    ///
    /// let listener = MyModel.changePublisher(in: db).sink { keys in
    ///     print("These primary keys may have changed: \(keys ?? "all")")
    /// }
    /// ```
    ///
    typealias ChangePublisher = PassthroughSubject<PrimaryKeyValues?, Never>

    internal static func isRelevantPrimaryKeyChange(watchedPrimaryKeys: Blackbird.PrimaryKeyValues?, changedPrimaryKeys: Blackbird.PrimaryKeyValues?) -> Bool {
        guard let watchedPrimaryKeys else {
            // Not watching any particular keys -- always update for any table change
            return true
        }
        
        guard let changedPrimaryKeys else {
            // Change sent for unknown/all keys -- always update
            return true
        }
        
        if !watchedPrimaryKeys.intersection(changedPrimaryKeys).isEmpty {
            // Overlapping keys -- update
            return true
        }
        
        return false
    }

    // MARK: - Legacy notifications

    static let legacyChangeNotification = NSNotification.Name("BlackbirdTableChangeNotification")
    static let legacyChangeNotificationTableKey = "BlackbirdChangedTable"
    static let legacyChangeNotificationPrimaryKeyValuesKey = "BlackbirdChangedPrimaryKeyValues"
}

// MARK: - Change publisher

extension Blackbird.Database {
    internal class ChangeReporter {
        private var lock = Blackbird.Lock()
        private var flushIsEnqueued = false
        private var activeTransactions = Set<Int64>()
        private var ignoreWritesToTableName: String? = nil
        private var accumulatedChangesPerKey: [String: Blackbird.PrimaryKeyValues] = [:]
        private var accumulatedChangesForEntireTables = Set<String>()
        private var tableChangePublishers: [String: Blackbird.ChangePublisher] = [:]
        
        private var sendLegacyChangeNotifications = false
        private var debugPrintEveryReportedChange = false
        
        init(options: Options) {
            debugPrintEveryReportedChange = options.contains(.debugPrintEveryReportedChange)
            sendLegacyChangeNotifications = options.contains(.sendLegacyChangeNotifications)
        }

        public func changePublisher(for tableName: String) -> Blackbird.ChangePublisher {
            lock.withLock {
                if let existing = tableChangePublishers[tableName] { return existing }
                let publisher = Blackbird.ChangePublisher()
                tableChangePublishers[tableName] = publisher
                return publisher
            }
        }

        public func ignoreWritesToTable(_ name: String) {
            lock.lock()
            ignoreWritesToTableName = name
            lock.unlock()
        }

        public func stopIgnoringWrites() {
            lock.lock()
            ignoreWritesToTableName = nil
            lock.unlock()
        }

        public func beginTransaction(_ transactionID: Int64) {
            lock.lock()
            activeTransactions.insert(transactionID)
            lock.unlock()
        }

        public func endTransaction(_ transactionID: Int64) {
            lock.lock()
            activeTransactions.remove(transactionID)
            if !flushIsEnqueued && activeTransactions.isEmpty && (!accumulatedChangesPerKey.isEmpty || !accumulatedChangesForEntireTables.isEmpty) {
                flushIsEnqueued = true
                DispatchQueue.main.async { [weak self] in self?.flush() }
            }
            lock.unlock()
        }

        public func reportChange(tableName: String, primaryKey: [Blackbird.Value]? = nil) {
            lock.lock()
            if tableName != ignoreWritesToTableName {
                if let primaryKey, !primaryKey.isEmpty {
                    if accumulatedChangesPerKey[tableName] == nil { accumulatedChangesPerKey[tableName] = Blackbird.PrimaryKeyValues() }
                    accumulatedChangesPerKey[tableName]!.insert(primaryKey)
                } else {
                    accumulatedChangesForEntireTables.insert(tableName)
                }

                if !flushIsEnqueued, activeTransactions.isEmpty {
                    flushIsEnqueued = true
                    DispatchQueue.main.async { [weak self] in self?.flush() }
                }
            }
            lock.unlock()
        }
        
        private func flush() {
            lock.lock()
            flushIsEnqueued = false
            let publishers = tableChangePublishers
            let byEntireTable = accumulatedChangesForEntireTables
            var byTableAndKeys = accumulatedChangesPerKey
            accumulatedChangesPerKey.removeAll()
            accumulatedChangesForEntireTables.removeAll()
            lock.unlock()

            for tableName in byEntireTable {
                if debugPrintEveryReportedChange { print("[Blackbird.ChangeReporter] changed \(tableName) (all/unknown)") }
                byTableAndKeys.removeValue(forKey: tableName)
                if let publisher = publishers[tableName] { publisher.send(nil) }
                if sendLegacyChangeNotifications { sendLegacyNotification(tableName: tableName, changedKeys: nil) }
            }
            
            for (tableName, keys) in byTableAndKeys {
                if debugPrintEveryReportedChange { print("[Blackbird.ChangeReporter] changed \(tableName) (\(keys.count) keys)") }
                if let publisher = publishers[tableName] { publisher.send(keys) }
                if sendLegacyChangeNotifications { sendLegacyNotification(tableName: tableName, changedKeys: keys) }
            }
        }
        
        private func sendLegacyNotification(tableName: String, changedKeys: Blackbird.PrimaryKeyValues?) {
            var userInfo: [AnyHashable: Any] = [Blackbird.legacyChangeNotificationTableKey: tableName]
            if let changedKeys {
                userInfo[Blackbird.legacyChangeNotificationPrimaryKeyValuesKey] = changedKeys.map { primaryKey in
                    primaryKey.map { $0.objcValue() }
                }
            }
            NotificationCenter.default.post(name: Blackbird.legacyChangeNotification, object: tableName, userInfo: userInfo)
        }
    }
}

// MARK: - General query cache with Combine publisher

extension Blackbird {
    public typealias CachedResultGenerator<T> = ((_ db: Blackbird.Database) async throws -> T)

    public class CachedResultPublisher<T> {
        public var valuePublisher = CurrentValueSubject<T?, Never>(nil)

        private var cacheLock = Lock()
        private var cachedResults: T? = nil
        
        private var tableName: String? = nil
        private var database: Blackbird.Database? = nil
        private var generator: CachedResultGenerator<T>? = nil
        private var tableChangePublisher: AnyCancellable? = nil

        public func subscribe(to tableName: String, in database: Blackbird.Database?, generator: CachedResultGenerator<T>?) {
            self.tableName = tableName
            self.generator = generator
            self.changeDatabase(database)
            enqueueUpdate()
        }
        
        private func update(_ cachedResults: T?) async throws {
            let results: T?
            if let cachedResults {
                results = cachedResults
            } else {
                results = (generator != nil && database != nil ? try await generator!(database!) : nil)
                cacheLock.withLock { self.cachedResults = results }
                await MainActor.run {
                    valuePublisher.send(results)
                }
            }
        }
        
        private func changeDatabase(_ newDatabase: Database?) {
            if newDatabase == database { return }
            database = newDatabase
            self.cacheLock.withLock { self.cachedResults = nil }
            
            if let database, let tableName {
                self.tableChangePublisher = database.changeReporter.changePublisher(for: tableName).sink { [weak self] _ in
                    guard let self else { return }
                    self.cacheLock.withLock { self.cachedResults = nil }
                    self.enqueueUpdate()
                }
            } else {
                self.tableChangePublisher = nil
            }
        }
        
        private func enqueueUpdate() {
            let cachedResults = cacheLock.withLock { self.cachedResults }
            Task { do { try await self.update(cachedResults) } catch { print("[Blackbird.ModelArrayUpdater<\(String(describing: T.self))>] ⚠️ Error updating: \(error.localizedDescription)") } }
        }
    }
}
