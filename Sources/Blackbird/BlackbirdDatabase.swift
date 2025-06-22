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
//  BlackbirdDatabase.swift
//  Created by Marco Arment on 11/28/22.
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

extension Blackbird {
    public enum TransactionResult<R: Sendable>: Sendable {
        case rolledBack
        case committed(R)
    }
}

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
    ///         Use ``Blackbird/Database/cancellableTransaction(_:)`` to roll back transactions without throwing errors.
    ///
    /// While inside the transaction's `action`:
    /// * Queries against the isolated ``Blackbird/Database/Core`` can be executed synchronously (using `try` instead of `try await`).
    /// * Change notifications for this database via ``Blackbird/ChangePublisher`` are queued until the transaction is completed. When delivered, multiple changes for the same table are consolidated into a single notification with every affected primary-key value.
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
    /// ``Blackbird/Database/cancellableTransaction(_:)``
    @discardableResult
    func transaction<R: Sendable>(_ action: (@Sendable (_ core: isolated Blackbird.Database.Core) async throws -> R) ) async throws -> R

    /// Equivalent to ``Blackbird/Database/transaction(_:)``, but with the ability to cancel.
    /// - Parameter action: The actions to perform in the transaction. Throw ``Blackbird/Error/cancelTransaction`` within the action to cancel and roll back the transaction. This error will not be rethrown.
    ///
    /// If any other error is thrown, the transaction is rolled back and the error is rethrown to the caller.
    ///
    /// See ``Blackbird/Database/transaction(_:)`` for details.
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
    @discardableResult
    func cancellableTransaction<R: Sendable>(_ action: (@Sendable (_ core: isolated Blackbird.Database.Core) async throws -> R) ) async throws -> Blackbird.TransactionResult<R>
    
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
    public final class Database: Identifiable, Hashable, Equatable, BlackbirdQueryable, @unchecked Sendable /* due to the `core` instance variable being set in init */ {
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
            case backupError(description: String)
            case queryArgumentNameError(query: String, name: String)
            case queryArgumentValueError(query: String, description: String)
            case queryExecutionError(query: String, description: String)
            case queryResultValueError(query: String, column: String)
            case uniqueConstraintFailed
            case databaseIsClosed
        }
        
        /// Options for customizing database behavior.
        public struct Options: OptionSet, Sendable {
            public let rawValue: Int
            public init(rawValue: Int) { self.rawValue = rawValue }

            internal static let inMemoryDatabase             = Options(rawValue: 1 << 0)

            /// Sets the database to read-only. Any calls to ``BlackbirdModel`` write functions with a read-only database will terminate with a fatal error.
            public static let readOnly                       = Options(rawValue: 1 << 1)

            /// Monitor for changes to the database file from outside of this connection, such as from a different process or a different SQLite library within the same process.
            public static let monitorForExternalChanges      = Options(rawValue: 1 << 2)

            /// Logs every query with `print()`. Useful for debugging.
            public static let debugPrintEveryQuery           = Options(rawValue: 1 << 3)

            /// When using ``debugPrintEveryQuery``, parameterized query values will be included in the logged query strings instead of their placeholders. Useful for debugging.
            public static let debugPrintQueryParameterValues = Options(rawValue: 1 << 4)

            /// Logs every change reported by ``Blackbird/ChangePublisher`` instances for this database with `print()`. Useful for debugging.
            public static let debugPrintEveryReportedChange  = Options(rawValue: 1 << 5)

            /// Logs cache hits and misses with `print()`. Useful for debugging.
            public static let debugPrintCacheActivity        = Options(rawValue: 1 << 6)

            /// Require the calling of ``BlackbirdModel/resolveSchema(in:)`` before any queries to a `BlackbirdModel` type.
            ///
            /// Without this option, schema validation and any needed migrations are performed upon the first query to a ``BlackbirdModel`` type.
            /// This is convenient, but has downsides:
            ///
            /// - Schema migrations occurring at unpredictable times may cause unpredictable performance.
            /// - The callsite for failed validations or schema migrations is unpredictable, making it difficult to build recovery logic.
            /// - If using multiple ``Blackbird/Database`` instances, subtle bugs may be introduced if a ``BlackbirdModel`` is inadvertently queried with the wrong database.
            ///
            /// With this option set, any ``BlackbirdModel`` type must first call ``BlackbirdModel/resolveSchema(in:)`` before any queries are performed against it for this database.
            ///
            /// If any queries are performed without first having called ``BlackbirdModel/resolveSchema(in:)``, a fatal error occurs.
            ///
            /// In addition to creating more predictable performance, this is useful to enforce the consolidation of schema validation and migrations to database-opening time so the caller can take appropriate action.
            ///
            /// ## Example
            /// ```swift
            /// do {
            ///     let db = try Blackbird.Database(path: …, options: [.requireModelSchemaValidationBeforeUse])
            ///
            ///     for modelType in [
            ///         // List all BlackbirdModel types to be used with this database:
            ///         Author.self,
            ///         Post.self,
            ///         Genre.self,
            ///     ] {
            ///         // Validate schema and attempt any needed migrations
            ///         try await modelType.resolveSchema(in: db)
            ///     }
            /// } catch {
            ///     // Perform appropriate recovery actions, such as
            ///     //  deleting the database file so it can be recreated:
            ///     try? Blackbird.Database.delete(atPath: …)
            /// }
            /// ```
            public static let requireModelSchemaValidationBeforeUse = Options(rawValue: 1 << 7)
        }
        
        /// Returns all filenames expected to be used by a database if created at the given file path.
        ///
        /// SQLite typically uses three files for a database:
        /// - The supplied path
        /// - A second file at the path with `-wal` appended
        /// - A third file at the path with `-shm` appended
        ///
        /// This method returns all three expected filenames based on the given path.
        public static func allFilePaths(for path: String) -> [String] {
            // Can't use sqlite3_filename_wal(), etc. because we don't have a DB connection.
            return [path, "\(path)-wal", "\(path)-shm"]
        }
        
        /// Delete the database files, if they exist, at the given path.
        ///
        /// > Note: This will delete multiple files. See ``allFilePaths(for:)``.
        public static func delete(atPath path: String) throws {
            for dbFilePath in allFilePaths(for: path) {
                if FileManager.default.fileExists(atPath: dbFilePath) {
                    try FileManager.default.removeItem(atPath: dbFilePath)
                }
            }
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
        
        /// The maximum number of parameters (`?`) supported in database queries. (The value of `SQLITE_LIMIT_VARIABLE_NUMBER` of the backing SQLite instance.)
        public let maxQueryVariableCount: Int

        internal var _core: Core? = nil
        internal var core: Core { _core! } // Set in init, so it's always there… sorry for the bad hack
        
        internal let changeReporter: ChangeReporter
        internal let cache: Cache
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
            // Use a local because we can't use self until everything has been initalized
            let performanceLog = PerformanceLogger(subsystem: Blackbird.loggingSubsystem, category: "Database")
            let spState = performanceLog.begin(signpost: .openDatabase)
            defer { performanceLog.end(state: spState) }

            var normalizedOptions = options
            if path.isEmpty || path == ":memory:" {
                normalizedOptions.insert(.inMemoryDatabase)
                normalizedOptions.remove(.monitorForExternalChanges)
            }

            let isUniqueInstanceForPath = normalizedOptions.contains(.inMemoryDatabase) || InstancePool.addInstance(path: path)
            if !isUniqueInstanceForPath { throw Error.anotherInstanceExistsWithPath(path: path) }
            id = InstancePool.nextInstanceID()

            self.options = normalizedOptions
            self.path = normalizedOptions.contains(.inMemoryDatabase) ? nil : path
            self.cache = Cache()
            self.changeReporter = ChangeReporter(options: options, cache: cache)

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
            
            self.maxQueryVariableCount = Int(sqlite3_limit(handle, SQLITE_LIMIT_VARIABLE_NUMBER, -1))
            
            if !normalizedOptions.contains(.readOnly), SQLITE_OK != sqlite3_exec(handle, "PRAGMA journal_mode = WAL", nil, nil, nil) || SQLITE_OK != sqlite3_exec(handle, "PRAGMA synchronous = NORMAL", nil, nil, nil) {
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

            sqlite3_update_hook(handle, { ctx, operation, dbName, tableName, rowid in
                guard let ctx else { return }
                let changeReporter = Unmanaged<ChangeReporter>.fromOpaque(ctx).takeUnretainedValue()
                changeReporter.numChangesReportedByUpdateHook += 1
                if let tableName, let tableNameStr = String(cString: tableName, encoding: .utf8) {
                    changeReporter.reportChange(tableName: tableNameStr, rowID: rowid, changedColumns: nil)
                }
            }, Unmanaged<ChangeReporter>.passUnretained(changeReporter).toOpaque())

            perfLog = performanceLog
            _core = Core(database: self, dbHandle: Core.SQLiteHandle(pointer: handle), changeReporter: changeReporter, cache: cache, fileChangeMonitor: fileChangeMonitor, options: options)

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

        @discardableResult
        public func transaction<R: Sendable>(_ action: (@Sendable (_ core: isolated Core) async throws -> R) ) async throws -> R { try await core.transaction(action) }

        @discardableResult
        public func cancellableTransaction<R: Sendable>(_ action: (@Sendable (_ core: isolated Core) async throws -> R) ) async throws -> Blackbird.TransactionResult<R> { try await core.cancellableTransaction(action) }

        @discardableResult public func query(_ query: String) async throws -> [Blackbird.Row] { return try await core.query(query, [Sendable]()) }

        @discardableResult public func query(_ query: String, _ arguments: Sendable...) async throws -> [Blackbird.Row] { return try await core.query(query, arguments) }

        @discardableResult public func query(_ query: String, arguments: [Sendable]) async throws -> [Blackbird.Row] { return try await core.query(query, arguments) }

        @discardableResult public func query(_ query: String, arguments: [String: Sendable]) async throws -> [Blackbird.Row] { return try await core.query(query, arguments: arguments) }

        public func setArtificialQueryDelay(_ delay: TimeInterval?) async { await core.setArtificialQueryDelay(delay) }

        /// Creates a backup of the whole database.
        ///
        /// - Parameters:
        ///   - targetPath: The path to the backup file to be created.
        ///   - pagesPerStep: The number of [pages](https://www.sqlite.org/fileformat.html#pages) to copy in a single step (optional; defaults to 100).
        ///
        /// An error will be thrown if a file already exists at `targetPath`,  the backup database cannot be created or the backup process fails.
        public func backup(to targetPath: String, pagesPerStep: Int32 = 100) async throws { try await core.backup(to: targetPath, pagesPerStep: pagesPerStep) }

        // MARK: - Core
        
        /// An actor for protected concurrent access to a database.
        public actor Core: BlackbirdQueryable {
            internal struct SQLiteHandle: @unchecked Sendable {
                let pointer: OpaquePointer
            }
        
            private struct PreparedStatement {
                let handle: SQLiteHandle
                let isReadOnly: Bool
            }
        
            private var debugPrintEveryQuery = false
            private var debugPrintQueryParameterValues = false

            internal let dbHandle: SQLiteHandle

            private weak var _database: Database?
            public func database() throws -> Database {
                guard let _database else { throw Blackbird.Database.Error.databaseIsClosed }
                return _database
            }
            
            private weak var changeReporter: ChangeReporter?
            private weak var fileChangeMonitor: FileChangeMonitor?
            private weak var cache: Cache?
            private var cachedStatements: [String: PreparedStatement] = [:]
            private var isClosed = false
            private var nextTransactionID: Int64 = 0

            private var dataVersionStmt: OpaquePointer? = nil
            private var previousDataVersion: Int64 = 0

            private var perfLog = PerformanceLogger(subsystem: Blackbird.loggingSubsystem, category: "Database.Core")

            internal init(database: Database, dbHandle: SQLiteHandle, changeReporter: ChangeReporter?, cache: Cache?, fileChangeMonitor: FileChangeMonitor?, options: Database.Options) {
                self._database = database
                self.dbHandle = dbHandle
                self.changeReporter = changeReporter
                self.fileChangeMonitor = fileChangeMonitor
                self.cache = cache
                self.debugPrintEveryQuery = options.contains(.debugPrintEveryQuery)
                self.debugPrintQueryParameterValues = options.contains(.debugPrintQueryParameterValues)
                
                if options.contains(.monitorForExternalChanges), SQLITE_OK == sqlite3_prepare_v3(dbHandle.pointer, "PRAGMA data_version", -1, UInt32(SQLITE_PREPARE_PERSISTENT), &dataVersionStmt, nil) {
                    if SQLITE_ROW == sqlite3_step(dataVersionStmt) { previousDataVersion = sqlite3_column_int64(dataVersionStmt, 0) }
                    sqlite3_reset(dataVersionStmt)
                }
            }

            deinit {
                if !isClosed {
                    for (_, statement) in cachedStatements { sqlite3_finalize(statement.handle.pointer) }
                    sqlite3_close(dbHandle.pointer)
                    isClosed = true
                }
            }
            
            fileprivate func close() {
                if isClosed { return }
                let spState = perfLog.begin(signpost: .closeDatabase)
                defer { perfLog.end(state: spState) }
                for (_, statement) in cachedStatements { sqlite3_finalize(statement.handle.pointer) }
                sqlite3_close(dbHandle.pointer)
                isClosed = true
            }
            
            private var artificialQueryDelay: TimeInterval? = nil
            public func setArtificialQueryDelay(_ delay: TimeInterval?) {
                artificialQueryDelay = delay
            }
            
            internal var changeCount: Int64 {
                get {
                    if #available(macOS 12.3, iOS 15.4, tvOS 15.4, watchOS 8.5, *) {
                        return Int64(sqlite3_total_changes64(dbHandle.pointer))
                    } else {
                        return Int64(sqlite3_total_changes(dbHandle.pointer))
                    }
                }
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
            public func transaction<R: Sendable>(_ action: (@Sendable (_ core: isolated Blackbird.Database.Core) async throws -> R) ) async throws -> R {
                let result = try await cancellableTransaction { core in
                    return try await action(core)
                }

                switch result {
                    case .committed(let r): return r
                    case .rolledBack: fatalError("should never get here")
                }
            }

            // Exactly like the function above, but requires action to be synchronous
            public func transaction<R: Sendable>(_ action: (@Sendable (_ core: isolated Blackbird.Database.Core) throws -> R) ) throws -> R {
                let result = try cancellableTransaction { core in
                    return try action(core)
                }

                switch result {
                    case .committed(let r): return r
                    case .rolledBack: fatalError("should never get here")
                }
            }

            private let asyncTransactionSemaphore = Blackbird.Semaphore(value: 1)

            // Exactly like the function below, but accepts an async action
            public func cancellableTransaction<R: Sendable>(_ action: (@Sendable (_ core: isolated Blackbird.Database.Core) async throws -> R) ) async throws -> Blackbird.TransactionResult<R> {
                await asyncTransactionSemaphore.wait()
                defer { asyncTransactionSemaphore.signal() }

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
                do {
                    let result: R = try await action(self)
                    try execute("RELEASE SAVEPOINT \"\(transactionID)\"")
                    return .committed(result)
                } catch Blackbird.Error.cancelTransaction {
                    try execute("ROLLBACK TO SAVEPOINT \"\(transactionID)\"")
                    cache?.invalidate()
                    return .rolledBack
                } catch {
                    try execute("ROLLBACK TO SAVEPOINT \"\(transactionID)\"")
                    cache?.invalidate()
                    throw error
                }
            }
            
            // Exactly like the function above, but requires action to be synchronous
            public func cancellableTransaction<R: Sendable>(_ action: (@Sendable (_ core: isolated Blackbird.Database.Core) throws -> R) ) throws -> Blackbird.TransactionResult<R> {
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
                do {
                    let result: R = try action(self)
                    try execute("RELEASE SAVEPOINT \"\(transactionID)\"")
                    return .committed(result)
                } catch Blackbird.Error.cancelTransaction {
                    try execute("ROLLBACK TO SAVEPOINT \"\(transactionID)\"")
                    cache?.invalidate()
                    return .rolledBack
                } catch {
                    try execute("ROLLBACK TO SAVEPOINT \"\(transactionID)\"")
                    cache?.invalidate()
                    throw error
                }
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
                
                try _checkForUpdateHookBypass {
                    let result = sqlite3_exec(dbHandle.pointer, query, nil, nil, nil)
                    if result != SQLITE_OK { throw Error.queryError(query: query, description: errorDesc(dbHandle.pointer)) }
                }
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
            
            // Check for SQLite changes occurring during the given operation that bypass the update_hook, such as
            // the truncate optimization: https://www.sqlite.org/lang_delete.html#the_truncate_optimization
            //
            // Thanks, Gwendal Roué of GRDB: https://hachyderm.io/@groue/110038488774903347
            private func _checkForUpdateHookBypass<T>(statement: PreparedStatement? = nil, _ action: (() throws -> T)) rethrows -> T {
                guard let changeReporter else { return try action() }
                if let statement, statement.isReadOnly { return try action() }

                let changeCountBefore = changeCount
                let changesReportedBefore = changeReporter.numChangesReportedByUpdateHook
                let result = try action()
                
                if changeCount != changeCountBefore, changesReportedBefore == changeReporter.numChangesReportedByUpdateHook {
                    // Catch the SQLite truncate optimization
                    changeReporter.reportEntireDatabaseChange()
                }
                
                return result
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
                    try value.bind(database: self, statement: statementHandle.pointer, index: Int32(idx), for: query)
                    idx += 1
                }
                
                return try _checkForUpdateHookBypass(statement: statement) {
                    try rowsByExecutingPreparedStatement(statement, from: query)
                }
            }

            @discardableResult
            public func query(_ query: String, arguments: [String: Sendable]) throws -> [Blackbird.Row] {
                if isClosed { throw Error.databaseIsClosed }
                let statement = try preparedStatement(query)
                let statementHandle = statement.handle
                for (name, any) in arguments {
                    let value = try Value.fromAny(any)
                    try value.bind(database: self, statement: statementHandle.pointer, name: name, for: query)
                }

                return try _checkForUpdateHookBypass(statement: statement) {
                    try rowsByExecutingPreparedStatement(statement, from: query)
                }
            }

            private func preparedStatement(_ query: String) throws -> PreparedStatement {
                if let cached = cachedStatements[query] { return cached }
                var statementHandle: OpaquePointer? = nil
                let result = sqlite3_prepare_v3(dbHandle.pointer, query, -1, UInt32(SQLITE_PREPARE_PERSISTENT), &statementHandle, nil)
                guard result == SQLITE_OK, let statementHandle else { throw Error.queryError(query: query, description: errorDesc(dbHandle.pointer)) }
                
                let statement = PreparedStatement(handle: SQLiteHandle(pointer: statementHandle), isReadOnly: sqlite3_stmt_readonly(statementHandle) > 0)
                cachedStatements[query] = statement
                return statement
            }
            
            private func rowsByExecutingPreparedStatement(_ statement: PreparedStatement, from query: String) throws -> [Blackbird.Row] {
                if debugPrintEveryQuery {
                    if debugPrintQueryParameterValues, let cStr = sqlite3_expanded_sql(statement.handle.pointer), let expandedQuery = String(cString: cStr, encoding: .utf8) {
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

                var result = sqlite3_step(statementHandle.pointer)
                
                guard result == SQLITE_ROW || result == SQLITE_DONE else {
                    sqlite3_reset(statementHandle.pointer)
                    sqlite3_clear_bindings(statementHandle.pointer)
                    switch result {
                        case SQLITE_CONSTRAINT: throw Error.uniqueConstraintFailed
                        default: throw Error.queryExecutionError(query: query, description: errorDesc(dbHandle.pointer))
                    }
                }

                let columnCount = sqlite3_column_count(statementHandle.pointer)
                if columnCount == 0 {
                    guard sqlite3_reset(statementHandle.pointer) == SQLITE_OK, sqlite3_clear_bindings(statementHandle.pointer) == SQLITE_OK else {
                        throw Error.queryExecutionError(query: query, description: errorDesc(dbHandle.pointer))
                    }
                    return []
                }
                
                var columnNames: [String] = []
                for i in 0 ..< columnCount {
                    guard let charPtr = sqlite3_column_name(statementHandle.pointer, i), case let name = String(cString: charPtr) else {
                        throw Error.queryExecutionError(query: query, description: errorDesc(dbHandle.pointer))
                    }
                    columnNames.append(name)
                }

                var rows: [Blackbird.Row] = []
                while result == SQLITE_ROW {
                    var row: Blackbird.Row = [:]
                    for i in 0 ..< Int(columnCount) {
                        switch sqlite3_column_type(statementHandle.pointer, Int32(i)) {
                            case SQLITE_NULL:    row[columnNames[i]] = .null
                            case SQLITE_INTEGER: row[columnNames[i]] = .integer(sqlite3_column_int64(statementHandle.pointer, Int32(i)))
                            case SQLITE_FLOAT:   row[columnNames[i]] = .double(sqlite3_column_double(statementHandle.pointer, Int32(i)))

                            case SQLITE_TEXT:
                                guard let charPtr = sqlite3_column_text(statementHandle.pointer, Int32(i)) else { throw Error.queryResultValueError(query: query, column: columnNames[i]) }
                                row[columnNames[i]] = .text(String(cString: charPtr))
            
                            case SQLITE_BLOB:
                                let byteLength = sqlite3_column_bytes(statementHandle.pointer, Int32(i))
                                if byteLength > 0 {
                                    guard let bytes = sqlite3_column_blob(statementHandle.pointer, Int32(i)) else { throw Error.queryResultValueError(query: query, column: columnNames[i]) }
                                    row[columnNames[i]] = .data(Data(bytes: bytes, count: Int(byteLength)))
                                } else {
                                    row[columnNames[i]] = .data(Data())
                                }

                            default: throw Error.queryExecutionError(query: query, description: errorDesc(dbHandle.pointer))
                        }
                    }
                    rows.append(row)

                    result = sqlite3_step(statementHandle.pointer)
                }
                if result != SQLITE_DONE { throw Error.queryExecutionError(query: query, description: errorDesc(dbHandle.pointer)) }
                
                guard sqlite3_reset(statementHandle.pointer) == SQLITE_OK, sqlite3_clear_bindings(statementHandle.pointer) == SQLITE_OK else {
                    throw Error.queryExecutionError(query: query, description: errorDesc(dbHandle.pointer))
                }
                return rows
            }

            public func backup(to targetPath: String, pagesPerStep: Int32, printProgress: Bool = false) async throws {
                guard !FileManager.default.fileExists(atPath: targetPath) else {
                    throw Blackbird.Database.Error.backupError(description: "File already exists at `\(targetPath)`")
                }
                
                var targetDbHandle: OpaquePointer? = nil
                let flags: Int32 = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE
                let openResult = sqlite3_open_v2(targetPath, &targetDbHandle, flags, nil)

                guard let targetDbHandle else {
                    throw Error.cannotOpenDatabaseAtPath(path: targetPath, description: "SQLite cannot allocate memory")
                }

                defer { sqlite3_close(targetDbHandle) }

                guard openResult == SQLITE_OK else {
                    let code = sqlite3_errcode(targetDbHandle)
                    let msg = String(cString: sqlite3_errmsg(targetDbHandle), encoding: .utf8) ?? "(unknown)"
                    sqlite3_close(targetDbHandle)
                    throw Error.cannotOpenDatabaseAtPath(path: targetPath, description: "SQLite error code \(code): \(msg)")
                }

                guard let backup = sqlite3_backup_init(targetDbHandle, "main", dbHandle.pointer, "main") else {
                    throw Blackbird.Database.Error.backupError(description: errorDesc(targetDbHandle))
                }
                
                defer { sqlite3_backup_finish(backup) }
                
                var stepResult = SQLITE_OK
                while stepResult == SQLITE_OK || stepResult == SQLITE_BUSY || stepResult == SQLITE_LOCKED {
                    stepResult = sqlite3_backup_step(backup, pagesPerStep)

                    if printProgress {
                        let remainingPages = sqlite3_backup_remaining(backup)
                        let totalPages = sqlite3_backup_pagecount(backup)
                        let backedUpPages = totalPages - remainingPages
                        print("Backed up \(backedUpPages) pages of \(totalPages)")
                    }
                    
                    await Task.yield()
                }

                guard stepResult == SQLITE_DONE else {
                    throw Blackbird.Database.Error.backupError(description: errorDesc(targetDbHandle))
                }
            }
        }
    }

}
