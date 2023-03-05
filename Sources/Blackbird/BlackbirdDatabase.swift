//
//  BlackbirdDatabase.swift
//  Created by Marco Arment on 11/28/22.
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
import SQLite3

internal protocol BlackbirdQueryable {
    /// Executes arbitrary SQL queries without returning a value.
    ///
    /// - Parameter query: The SQL string to execute. May contain multiple queries separated by semicolons (`;`).
    ///
    /// Queries are passed to SQLite without any additional parameters or automatic replacements.
    ///
    /// Any type of query valid in SQLite may be used here.
    ///
    /// ## Example
    /// ```swift
    /// try await db.execute("PRAGMA user_version = 1; UPDATE posts SET deleted = 0")
    /// ```
    func execute(_ query: String) async throws
    
    /// Performs an atomic, cancellable transaction with synchronous database access and batched change notifications.
    /// - Parameters:
    ///     - action: The actions to perform in the transaction. If an error is thrown, the transaction is rolled back and the error is rethrown to the caller.
    ///    
    ///         Use ``cancellableTransaction(_:)`` to roll back transactions without throwing errors.
    ///
    /// While inside the transaction's `action`:
    /// * Queries against the isolated ``Blackbird/Database/Core`` can be executed synchronously (using `try` instead of `try await`).
    /// * Change notifications for this database, via both ``Blackbird/ChangePublisher`` and ``Blackbird/legacyChangeNotification``, are queued until the transaction is completed. When delivered, multiple changes for the same table are consolidated into a single notification with every affected primary-key value.
    ///
    ///     __Note:__ Notifications may be sent for changes occurring during the transaction even if the transaction is rolled back.
    ///
    /// ## Example
    /// ```swift
    /// try await db.transaction { core in
    ///     try core.query("INSERT INTO posts (id, title) VALUES (?, ?)", 1, "Title 1")
    ///     try core.query("INSERT INTO posts (id, title) VALUES (?, ?)", 2, "Title 2")
    ///     //...
    ///     try core.query("INSERT INTO posts (id, title) VALUES (?, ?)", 999, "Title 999")
    /// }
    /// ```
    ///
    /// > Performing large quantities of database writes is typically much faster inside a transaction.
    ///
    /// ## See also
    /// ``cancellableTransaction(_:)``
    func transaction(_ action: (@Sendable (_ core: isolated Blackbird.Database.Core) async throws -> Void) ) async throws

    /// Equivalent to ``transaction(_:)``, but with the ability to cancel without throwing an error.
    /// - Parameter action: The actions to perform in the transaction. Return `true` to commit the transaction or `false` to roll it back. If an error is thrown, the transaction is rolled back and the error is rethrown to the caller.
    ///
    /// See ``transaction(_:)`` for details.
    ///
    /// ## Example
    /// ```swift
    /// try await db.cancellableTransaction { core in
    ///     try core.query("INSERT INTO posts (id, title) VALUES (?, ?)", 1, "Title 1")
    ///     try core.query("INSERT INTO posts (id, title) VALUES (?, ?)", 2, "Title 2")
    ///     //...
    ///     try core.query("INSERT INTO posts (id, title) VALUES (?, ?)", 999, "Title 999")
    ///
    ///     let areWeReadyForCommitment: Bool = //...
    ///     return areWeReadyForCommitment
    /// }
    /// ```
    func cancellableTransaction(_ action: (@Sendable (_ core: isolated Blackbird.Database.Core) async throws -> Bool) ) async throws
    
    /// Queries the database.
    /// - Parameter query: An SQL query.
    /// - Returns: An array of rows matching the query if applicable, or an empty array otherwise.
    ///
    /// ## Example
    /// ```swift
    /// let ids = try await db.query("SELECT id FROM posts WHERE state = 1")
    /// ```
    @discardableResult func query(_ query: String) async throws -> [Blackbird.Row]
    
    /// Queries the database with an optional list of arguments.
    /// - Parameters:
    ///   - query: An SQL query that may contain placeholders specified as a question mark (`?`).
    ///   - arguments: Values corresponding to any placeholders in the query.
    /// - Returns: An array of rows matching the query if applicable, or an empty array otherwise.
    ///
    /// ## Example
    /// ```swift
    /// let rows = try await db.query(
    ///     "SELECT id FROM posts WHERE state = ? OR title = ?",
    ///     1,           // value for state
    ///     "Test Title" // value for title
    /// )
    /// ```
    @discardableResult func query(_ query: String, _ arguments: Sendable...) async throws -> [Blackbird.Row]
    
    /// Queries the database with an array of arguments.
    /// - Parameters:
    ///   - query: An SQL query that may contain placeholders specified as a question mark (`?`).
    ///   - arguments: An array of values corresponding to any placeholders in the query.
    /// - Returns: An array of rows matching the query if applicable, or an empty array otherwise.
    ///
    /// ## Example
    /// ```swift
    /// let rows = try await db.query(
    ///     "SELECT id FROM posts WHERE state = ? OR title = ?",
    ///     arguments: [1 /* value for state */, "Test Title" /* value for title */]
    /// )
    /// ```
    @discardableResult func query(_ query: String, arguments: [Sendable]) async throws -> [Blackbird.Row]
    
    /// Queries the database using a dictionary of named arguments.
    ///
    /// - Parameters:
    ///   - query: An SQL query that may contain named placeholders prefixed by a colon (`:`), at-sign (`@`), or dollar sign (`$`) as described in the [SQLite documentation](https://www.sqlite.org/c3ref/bind_blob.html).
    ///   - arguments: A dictionary of placeholder names used in the query and their corresponding values. Names must include the prefix character used.
    /// - Returns: An array of rows matching the query if applicable, or an empty array otherwise.
    ///
    /// ## Example
    /// ```swift
    /// let rows = try await db.query(
    ///     "SELECT id FROM posts WHERE state = :state OR title = :title",
    ///     arguments: [":state": 1, ":title": "Test Title"]
    /// )
    /// ```
    @discardableResult func query(_ query: String, arguments: [String: Sendable]) async throws -> [Blackbird.Row]
}

extension Blackbird {
    /// A managed SQLite database.
    ///
    /// A lightweight wrapper around [SQLite](https://www.sqlite.org/).
    ///
    /// ### Basic usage
    /// The database is accessed primarily via `async` calls, internally using an `actor` for performance, concurrency, and isolation.
    ///
    /// ```swift
    /// let db = try Blackbird.Database(path: "/tmp/test.sqlite")
    ///
    /// // SELECT with structured arguments and returned rows
    /// for row in try await db.query("SELECT id FROM posts WHERE state = ?", 1) {
    ///     let id = row["id"]
    ///     // ...
    /// }
    ///
    /// // Run direct queries
    /// try await db.execute("UPDATE posts SET comments = NULL")
    /// ```
    ///
    /// ### Synchronous transactions
    /// The isolated actor can also be accessed from ``transaction(_:)`` for synchronous functionality or high-performance batch operations:
    /// ```swift
    /// try await db.transaction { core in
    ///     try core.query("INSERT INTO posts VALUES (?, ?)", 16, "Sports!")
    ///     try core.query("INSERT INTO posts VALUES (?, ?)", 17, "Dewey Defeats Truman")
    ///     //...
    ///     try core.query("INSERT INTO posts VALUES (?, ?)", 89, "Florida Man At It Again")
    /// }
    /// ```
    ///
    public final class Database: Identifiable, Hashable, Equatable, BlackbirdQueryable, Sendable {
        /// Process-unique identifiers for Database instances. Used internally.
        public typealias InstanceID = Int64

        /// A process-unique identifier for this instance. Used internally.
        public let id: InstanceID
        
        public static func == (lhs: Database, rhs: Database) -> Bool { return lhs.id == rhs.id }
        
        public func hash(into hasher: inout Hasher) { hasher.combine(id) }

        public enum Error: Swift.Error {
            case anotherInstanceExistsWithPath(path: String)
            case cannotOpenDatabaseAtPath(path: String, description: String)
            case unsupportedConfigurationAtPath(path: String)
            case queryError(query: String, description: String)
            case queryArgumentNameError(query: String, name: String)
            case queryArgumentValueError(query: String, description: String)
            case queryExecutionError(query: String, description: String)
            case queryResultValueError(query: String, column: String)
            case databaseIsClosed
        }
        
        /// Options for customizing database behavior.
        public struct Options: OptionSet, Sendable {
            public let rawValue: Int
            public init(rawValue: Int) { self.rawValue = rawValue }

            internal static let inMemoryDatabase             = Options(rawValue: 1 << 0)

            /// Sets the database to read-only. Any calls to ``BlackbirdModel`` write functions with a read-only database will terminate with a fatal error.
            public static let readOnly                       = Options(rawValue: 1 << 1)
            
            /// Logs every query with `print()`. Useful for debugging.
            public static let debugPrintEveryQuery           = Options(rawValue: 1 << 2)

            /// When using ``debugPrintEveryQuery``, parameterized query values will be included in the logged query strings instead of their placeholders. Useful for debugging.
            public static let debugPrintQueryParameterValues = Options(rawValue: 1 << 3)

            /// Logs every change reported by ``Blackbird/ChangePublisher`` instances for this database with `print()`. Useful for debugging.
            public static let debugPrintEveryReportedChange  = Options(rawValue: 1 << 4)
            
            /// Sends ``Blackbird/legacyChangeNotification`` notifications using `NotificationCenter`.
            public static let sendLegacyChangeNotifications  = Options(rawValue: 1 << 5)

            /// Monitor for changes to the database file from outside of this connection, such as from a different process or a different SQLite library within the same process.
            public static let monitorForExternalChanges      = Options(rawValue: 1 << 6)
        }
        
        internal final class InstancePool: Sendable {
            private static let lock = Lock()
            private static let _nextInstanceID = Locked<InstanceID>(0)
            private static let pathsOfCurrentInstances = Locked(Set<String>())

            internal static func nextInstanceID() -> InstanceID {
                _nextInstanceID.withLock { $0 += 1; return $0 }
            }

            internal static func addInstance(path: String) -> Bool {
                pathsOfCurrentInstances.withLock { let (inserted, _) = $0.insert(path) ; return inserted }
            }

            internal static func removeInstance(path: String) {
                pathsOfCurrentInstances.withLock { $0.remove(path) }
            }
        }

        /// The path to the database file, or `nil` for in-memory databases.
        public let path: String?
        
        /// The ``Options-swift.struct`` used to create the database.
        public let options: Options

        internal let core: Core
        internal let changeReporter: ChangeReporter
        internal let perfLog: PerformanceLogger
        internal let fileChangeMonitor: FileChangeMonitor?
                
        private let isClosedLocked = Locked(false)
        
        /// Whether ``close()`` has been called on this database yet. Does **not** indicate whether the close operation has completed.
        ///
        /// > Note: Once an instance is closed, it is never reopened.
        public var isClosed: Bool {
            get { isClosedLocked.value }
        }

        /// Instantiates a new SQLite database in memory, without persisting to a file.
        public static func inMemoryDatabase(options: Options = []) throws -> Database {
            return try Database(path: "", options: options.union([.inMemoryDatabase]))
        }
        
        /// Instantiates a new SQLite database as a file on disk.
        ///
        /// - Parameters:
        ///   - path: The path to the database file. If no file exists at `path`, it will be created.
        ///   - options: Any custom behavior desired.
        ///
        /// At most one instance per database filename may exist at a time.
        ///
        /// An error will be thrown if another instance exists with the same filename, the database cannot be created, or the linked version of SQLite lacks the required capabilities.
        public init(path: String, options: Options = []) throws {
            let isUniqueInstanceForPath = options.contains(.inMemoryDatabase) || InstancePool.addInstance(path: path)
            if !isUniqueInstanceForPath { throw Error.anotherInstanceExistsWithPath(path: path) }
            id = InstancePool.nextInstanceID()

            // Use a local because we can't use self until everything has been initalized
            let performanceLog = PerformanceLogger(subsystem: Blackbird.loggingSubsystem, category: "Database")
            let spState = performanceLog.begin(signpost: .openDatabase)
            defer { performanceLog.end(state: spState) }

            var normalizedOptions = options
            if path.isEmpty || path == ":memory:" {
                normalizedOptions.insert(.inMemoryDatabase)
                normalizedOptions.remove(.monitorForExternalChanges)
            }

            self.options = normalizedOptions
            self.path = normalizedOptions.contains(.inMemoryDatabase) ? nil : path
            self.changeReporter = ChangeReporter(options: options)

            var handle: OpaquePointer? = nil
            let flags: Int32 = (options.contains(.readOnly) ? SQLITE_OPEN_READONLY : SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE) | SQLITE_OPEN_NOMUTEX
            let result = sqlite3_open_v2(self.path ?? ":memory:", &handle, flags, nil)
            guard let handle else {
                if let path = self.path { InstancePool.removeInstance(path: path) }
                throw Error.cannotOpenDatabaseAtPath(path: path, description: "SQLite cannot allocate memory")
            }
            guard result == SQLITE_OK else {
                let code = sqlite3_errcode(handle)
                let msg = String(cString: sqlite3_errmsg(handle), encoding: .utf8) ?? "(unknown)"
                sqlite3_close(handle)
                if let path = self.path { InstancePool.removeInstance(path: path) }
                throw Error.cannotOpenDatabaseAtPath(path: path, description: "SQLite error code \(code): \(msg)")
            }
            
            if SQLITE_OK != sqlite3_exec(handle, "PRAGMA journal_mode = WAL", nil, nil, nil) || SQLITE_OK != sqlite3_exec(handle, "PRAGMA synchronous = NORMAL", nil, nil, nil) {
                sqlite3_close(handle)
                if let path = self.path { InstancePool.removeInstance(path: path) }
                throw Error.unsupportedConfigurationAtPath(path: path)
            }

            if options.contains(.monitorForExternalChanges), let sqliteFilenameRef = sqlite3_db_filename(handle, nil) {
                fileChangeMonitor = FileChangeMonitor()
                
                if let cStr = sqlite3_filename_database(sqliteFilenameRef), let dbFilename = String(cString: cStr, encoding: .utf8), !dbFilename.isEmpty {
                    fileChangeMonitor?.addFile(filePath: dbFilename)
                }

                if let cStr = sqlite3_filename_wal(sqliteFilenameRef), let walFilename = String(cString: cStr, encoding: .utf8), !walFilename.isEmpty {
                    fileChangeMonitor?.addFile(filePath: walFilename)
                }
            } else {
                fileChangeMonitor = nil
            }

            core = Core(handle, changeReporter: changeReporter, fileChangeMonitor: fileChangeMonitor, options: options)
            perfLog = performanceLog
            
            sqlite3_update_hook(handle, { ctx, operation, dbName, tableName, rowid in
                guard let ctx else { return }
                let changeReporter = Unmanaged<ChangeReporter>.fromOpaque(ctx).takeUnretainedValue()
                if let tableName, let tableNameStr = String(cString: tableName, encoding: .utf8) {
                    changeReporter.reportChange(tableName: tableNameStr, changedColumns: nil)
                }
            }, Unmanaged<ChangeReporter>.passUnretained(changeReporter).toOpaque())

            fileChangeMonitor?.onChange { [weak self] in
                guard let self else { return }
                Task { await self.core.checkForExternalDatabaseChange() }
            }
        }
        
        deinit {
            if let path { InstancePool.removeInstance(path: path) }
        }
        
        /// Close the current database manually.
        ///
        /// Optional. If not called, databases automatically close when deallocated.
        ///
        /// This is useful if actions must be taken after the database is definitely closed, such as moving it, deleting it, or instantiating another ``Blackbird/Database`` instance for the same file.
        ///
        /// Sending any queries to a closed database throws an error.
        public func close() async {
            let spState = perfLog.begin(signpost: .closeDatabase)
            defer { perfLog.end(state: spState) }

            isClosedLocked.value = true
            await core.close()

            if let path { InstancePool.removeInstance(path: path) }
        }
        
        // MARK: - Forwarded Core functions
        
        public func execute(_ query: String) async throws { try await core.execute(query) }

        public func transaction(_ action: (@Sendable (_ core: isolated Core) async throws -> Void) ) async throws { try await core.transaction(action) }

        public func cancellableTransaction(_ action: (@Sendable (_ core: isolated Core) async throws -> Bool) ) async throws { try await core.cancellableTransaction(action) }

        @discardableResult public func query(_ query: String) async throws -> [Blackbird.Row] { return try await core.query(query, [Sendable]()) }

        @discardableResult public func query(_ query: String, _ arguments: Sendable...) async throws -> [Blackbird.Row] { return try await core.query(query, arguments) }

        @discardableResult public func query(_ query: String, arguments: [Sendable]) async throws -> [Blackbird.Row] { return try await core.query(query, arguments) }

        @discardableResult public func query(_ query: String, arguments: [String: Sendable]) async throws -> [Blackbird.Row] { return try await core.query(query, arguments) }

        public func setArtificialQueryDelay(_ delay: TimeInterval?) async { await core.setArtificialQueryDelay(delay) }


        // MARK: - Core
        
        /// An actor for protected concurrent access to a database.
        public actor Core: BlackbirdQueryable {
            private struct PreparedStatement {
                let handle: OpaquePointer
                let isReadOnly: Bool
            }
        
            private var debugPrintEveryQuery = false
            private var debugPrintQueryParameterValues = false

            internal var dbHandle: OpaquePointer
            private weak var changeReporter: ChangeReporter?
            private weak var fileChangeMonitor: FileChangeMonitor?
            private var cachedStatements: [String: PreparedStatement] = [:]
            private var isClosed = false
            private var nextTransactionID: Int64 = 0

            private var dataVersionStmt: OpaquePointer? = nil
            private var previousDataVersion: Int64 = 0

            private var perfLog = PerformanceLogger(subsystem: Blackbird.loggingSubsystem, category: "Database.Core")

            internal init(_ dbHandle: OpaquePointer, changeReporter: ChangeReporter?, fileChangeMonitor: FileChangeMonitor?, options: Database.Options) {
                self.dbHandle = dbHandle
                self.changeReporter = changeReporter
                self.fileChangeMonitor = fileChangeMonitor
                self.debugPrintEveryQuery = options.contains(.debugPrintEveryQuery)
                self.debugPrintQueryParameterValues = options.contains(.debugPrintQueryParameterValues)
                
                if options.contains(.monitorForExternalChanges), SQLITE_OK == sqlite3_prepare_v3(dbHandle, "PRAGMA data_version", -1, UInt32(SQLITE_PREPARE_PERSISTENT), &dataVersionStmt, nil) {
                    if SQLITE_ROW == sqlite3_step(dataVersionStmt) { previousDataVersion = sqlite3_column_int64(dataVersionStmt, 0) }
                    sqlite3_reset(dataVersionStmt)
                }
            }

            deinit {
                if !isClosed {
                    for (_, statement) in cachedStatements { sqlite3_finalize(statement.handle) }
                    sqlite3_close(dbHandle)
                    isClosed = true
                }
            }
            
            fileprivate func close() {
                if isClosed { return }
                let spState = perfLog.begin(signpost: .closeDatabase)
                defer { perfLog.end(state: spState) }
                for (_, statement) in cachedStatements { sqlite3_finalize(statement.handle) }
                sqlite3_close(dbHandle)
                isClosed = true
            }
            
            private var artificialQueryDelay: TimeInterval? = nil
            public func setArtificialQueryDelay(_ delay: TimeInterval?) {
                artificialQueryDelay = delay
            }
                        
            internal func checkForExternalDatabaseChange() {
                guard let dataVersionStmt else { return }
                if debugPrintEveryQuery { print("[Blackbird.Database] PRAGMA data_version") }
                
                var newVersion: Int64 = 0
                if SQLITE_ROW == sqlite3_step(dataVersionStmt) { newVersion = sqlite3_column_int64(dataVersionStmt, 0) }
                sqlite3_reset(dataVersionStmt)

                if newVersion != previousDataVersion {
                    previousDataVersion = newVersion
                    changeReporter?.reportEntireDatabaseChange()
                }
            }

            // Exactly like the function below, but accepts an async action
            public func transaction(_ action: (@Sendable (_ core: isolated Blackbird.Database.Core) throws -> Void) ) throws {
                try cancellableTransaction { core in
                    try action(core)
                    return true
                }
            }

            // Exactly like the function above, but requires action to be synchronous
            public func transaction(_ action: (@Sendable (_ core: isolated Blackbird.Database.Core) async throws -> Void) ) async throws {
                try await cancellableTransaction { core in
                    try await action(core)
                    return true
                }
            }

            // Exactly like the function below, but accepts an async action
            public func cancellableTransaction(_ action: (@Sendable (_ core: isolated Blackbird.Database.Core) async throws -> Bool) ) async throws {
                if isClosed { throw Error.databaseIsClosed }
                let transactionID = nextTransactionID
                nextTransactionID += 1
                changeReporter?.beginTransaction(transactionID)
                fileChangeMonitor?.beginExpectedChange(transactionID)
                defer {
                    changeReporter?.endTransaction(transactionID)
                    fileChangeMonitor?.endExpectedChange(transactionID)
                    checkForExternalDatabaseChange()
                }

                let spState = perfLog.begin(signpost: .cancellableTransaction, message: "Transaction ID: \(transactionID)")
                defer { perfLog.end(state: spState) }

                try execute("SAVEPOINT \"\(transactionID)\"")
                var commit = false
                do { commit = try await action(self) }
                catch {
                    try execute("ROLLBACK TO SAVEPOINT \"\(transactionID)\"")
                    throw error
                }

                if commit { try execute("RELEASE SAVEPOINT \"\(transactionID)\"") }
                else { try execute("ROLLBACK TO SAVEPOINT \"\(transactionID)\"") }
            }
            
            // Exactly like the function above, but requires action to be synchronous
            public func cancellableTransaction(_ action: (@Sendable (_ core: isolated Blackbird.Database.Core) throws -> Bool) ) throws {
                if isClosed { throw Error.databaseIsClosed }
                let transactionID = nextTransactionID
                nextTransactionID += 1
                changeReporter?.beginTransaction(transactionID)
                fileChangeMonitor?.beginExpectedChange(transactionID)
                defer {
                    changeReporter?.endTransaction(transactionID)
                    fileChangeMonitor?.endExpectedChange(transactionID)
                    checkForExternalDatabaseChange()
                }

                let spState = perfLog.begin(signpost: .cancellableTransaction, message: "Transaction ID: \(transactionID)")
                defer { perfLog.end(state: spState) }

                try execute("SAVEPOINT \"\(transactionID)\"")
                var commit = false
                do { commit = try action(self) }
                catch {
                    try execute("ROLLBACK TO SAVEPOINT \"\(transactionID)\"")
                    throw error
                }

                if commit { try execute("RELEASE SAVEPOINT \"\(transactionID)\"") }
                else { try execute("ROLLBACK TO SAVEPOINT \"\(transactionID)\"") }
            }
            
            public func execute(_ query: String) throws {
                if debugPrintEveryQuery { print("[Blackbird.Database] \(query)") }
                if isClosed { throw Error.databaseIsClosed }

                let spState = perfLog.begin(signpost: .execute, message: query)
                defer { perfLog.end(state: spState) }

                if let artificialQueryDelay { Thread.sleep(forTimeInterval: artificialQueryDelay) }

                let transactionID = nextTransactionID
                nextTransactionID += 1
                changeReporter?.beginTransaction(transactionID)
                fileChangeMonitor?.beginExpectedChange(transactionID)
                defer {
                    changeReporter?.endTransaction(transactionID)
                    fileChangeMonitor?.endExpectedChange(transactionID)
                    checkForExternalDatabaseChange()
                }
                
                let result = sqlite3_exec(dbHandle, query, nil, nil, nil)
                if result != SQLITE_OK { throw Error.queryError(query: query, description: errorDesc(dbHandle)) }
            }
            
            nonisolated internal func errorDesc(_ dbHandle: OpaquePointer?, _ query: String? = nil) -> String {
                guard let dbHandle else { return "No SQLite handle" }
                let code = sqlite3_errcode(dbHandle)
                let msg = String(cString: sqlite3_errmsg(dbHandle), encoding: .utf8) ?? "(unknown)"

                if #available(iOS 16, watchOS 9, macOS 13, tvOS 16, *), case let offset = sqlite3_error_offset(dbHandle), offset >= 0 {
                    return "SQLite error code \(code) at index \(offset): \(msg)"
                } else {
                    return "SQLite error code \(code): \(msg)"
                }
            }

            @discardableResult
            public func query(_ query: String) throws -> [Blackbird.Row] { return try self.query(query, [Sendable]()) }

            @discardableResult
            public func query(_ query: String, _ arguments: Sendable...) throws -> [Blackbird.Row] { return try self.query(query, arguments: arguments) }

            @discardableResult
            public func query(_ query: String, arguments: [Sendable]) throws -> [Blackbird.Row] {
                if isClosed { throw Error.databaseIsClosed }
                let statement = try preparedStatement(query)
                let statementHandle = statement.handle
                var idx = 1 // SQLite bind-parameter indexes start at 1, not 0!
                for any in arguments {
                    let value = try Value.fromAny(any)
                    try value.bind(database: self, statement: statementHandle, index: Int32(idx), for: query)
                    idx += 1
                }
                return try rowsByExecutingPreparedStatement(statement, from: query)
            }

            @discardableResult
            public func query(_ query: String, arguments: [String: Sendable]) throws -> [Blackbird.Row] {
                if isClosed { throw Error.databaseIsClosed }
                let statement = try preparedStatement(query)
                let statementHandle = statement.handle
                for (name, any) in arguments {
                    let value = try Value.fromAny(any)
                    try value.bind(database: self, statement: statementHandle, name: name, for: query)
                }
                return try rowsByExecutingPreparedStatement(statement, from: query)
            }

            private func preparedStatement(_ query: String) throws -> PreparedStatement {
                if let cached = cachedStatements[query] { return cached }
                var statementHandle: OpaquePointer? = nil
                let result = sqlite3_prepare_v3(dbHandle, query, -1, UInt32(SQLITE_PREPARE_PERSISTENT), &statementHandle, nil)
                guard result == SQLITE_OK, let statementHandle else { throw Error.queryError(query: query, description: errorDesc(dbHandle)) }
                
                let statement = PreparedStatement(handle: statementHandle, isReadOnly: sqlite3_stmt_readonly(statementHandle) > 0)
                cachedStatements[query] = statement
                return statement
            }
            
            private func rowsByExecutingPreparedStatement(_ statement: PreparedStatement, from query: String) throws -> [Blackbird.Row] {
                if debugPrintEveryQuery {
                    if debugPrintQueryParameterValues, let cStr = sqlite3_expanded_sql(statement.handle), let expandedQuery = String(cString: cStr, encoding: .utf8) {
                        print("[Blackbird.Database] \(expandedQuery)")
                    } else {
                        print("[Blackbird.Database] \(query)")
                    }
                }
                let statementHandle = statement.handle

                let spState = perfLog.begin(signpost: .rowsByPreparedFunc, message: query)
                defer { perfLog.end(state: spState) }

                if let artificialQueryDelay { Thread.sleep(forTimeInterval: artificialQueryDelay) }

                let transactionID = nextTransactionID
                nextTransactionID += 1
                changeReporter?.beginTransaction(transactionID)
                if !statement.isReadOnly { fileChangeMonitor?.beginExpectedChange(transactionID) }
                defer {
                    changeReporter?.endTransaction(transactionID)
                    if !statement.isReadOnly {
                        fileChangeMonitor?.endExpectedChange(transactionID)
                        checkForExternalDatabaseChange()
                    }
                }

                var result = sqlite3_step(statementHandle)
                
                guard result == SQLITE_ROW || result == SQLITE_DONE else { throw Error.queryExecutionError(query: query, description: errorDesc(dbHandle)) }

                let columnCount = sqlite3_column_count(statementHandle)
                if columnCount == 0 {
                    guard sqlite3_reset(statementHandle) == SQLITE_OK, sqlite3_clear_bindings(statementHandle) == SQLITE_OK else {
                        throw Error.queryExecutionError(query: query, description: errorDesc(dbHandle))
                    }
                    return []
                }
                
                var columnNames: [String] = []
                for i in 0 ..< columnCount {
                    guard let charPtr = sqlite3_column_name(statementHandle, i), case let name = String(cString: charPtr) else {
                        throw Error.queryExecutionError(query: query, description: errorDesc(dbHandle))
                    }
                    columnNames.append(name)
                }

                var rows: [Blackbird.Row] = []
                while result == SQLITE_ROW {
                    var row: Blackbird.Row = [:]
                    for i in 0 ..< Int(columnCount) {
                        switch sqlite3_column_type(statementHandle, Int32(i)) {
                            case SQLITE_NULL:    row[columnNames[i]] = .null
                            case SQLITE_INTEGER: row[columnNames[i]] = .integer(sqlite3_column_int64(statementHandle, Int32(i)))
                            case SQLITE_FLOAT:   row[columnNames[i]] = .double(sqlite3_column_double(statementHandle, Int32(i)))

                            case SQLITE_TEXT:
                                guard let charPtr = sqlite3_column_text(statementHandle, Int32(i)) else { throw Error.queryResultValueError(query: query, column: columnNames[i]) }
                                row[columnNames[i]] = .text(String(cString: charPtr))
            
                            case SQLITE_BLOB:
                                let byteLength = sqlite3_column_bytes(statementHandle, Int32(i))
                                if byteLength > 0 {
                                    guard let bytes = sqlite3_column_blob(statementHandle, Int32(i)) else { throw Error.queryResultValueError(query: query, column: columnNames[i]) }
                                    row[columnNames[i]] = .data(Data(bytes: bytes, count: Int(byteLength)))
                                } else {
                                    row[columnNames[i]] = .data(Data())
                                }

                            default: throw Error.queryExecutionError(query: query, description: errorDesc(dbHandle))
                        }
                    }
                    rows.append(row)

                    result = sqlite3_step(statementHandle)
                }
                if result != SQLITE_DONE { throw Error.queryExecutionError(query: query, description: errorDesc(dbHandle)) }
                
                guard sqlite3_reset(statementHandle) == SQLITE_OK, sqlite3_clear_bindings(statementHandle) == SQLITE_OK else {
                    throw Error.queryExecutionError(query: query, description: errorDesc(dbHandle))
                }
                return rows
            }
        }
    }

}
