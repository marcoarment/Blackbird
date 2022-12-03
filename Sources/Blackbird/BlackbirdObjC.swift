//
//  BlackbirdObjC.swift
//  Created by Marco Arment on 11/29/22.
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
//
//  ***************************************************************************************************
//  *                                                                                                 *
//  *    This file can be omitted from projects that don't need Blackbird access from Objective-C.    *
//  *                                                                                                 *
//  ***************************************************************************************************
//

import Foundation

fileprivate func raiseObjCException(_ error: Error) -> Never {
    NSException(name: NSExceptionName(rawValue: "BlackbirdException"), reason: error.localizedDescription).raise()
    fatalError() // will never execute, but tricks Swift into accepting the Never return type
}

extension Blackbird.Value {
    internal func objcValue() -> NSObject {
        switch self {
            case .null:           return NSNull()
            case let .integer(i): return NSNumber(value: i)
            case let .double(d):  return NSNumber(value: d)
            case let .text(s):    return NSString(string: s)
            case let .data(d):    return NSData(data: d)
        }
    }
}

extension Blackbird.Row {
    internal func objcRow() -> [String: NSObject] { self.mapValues { $0.objcValue() } }
}

/// Objective-C version of ``Blackbird/ColumnType``.
@objc public enum BlackbirdColumnTypeObjC: Int {
    case integer
    case double
    case text
    case data
    
    internal func columnType() -> Blackbird.ColumnType {
        switch self {
            case .integer: return Blackbird.ColumnType.integer
            case .double:  return Blackbird.ColumnType.double
            case .text:    return Blackbird.ColumnType.text
            case .data:    return Blackbird.ColumnType.data
        }
    }
}

/// Objective-C wrapper for ``Blackbird/Column``.
@objc public class BlackbirdColumnObjC: NSObject {
    @objc public let name: String
    internal let column: Blackbird.Column
    
    /// Objective-C version of ``Blackbird/Column/init(name:type:mayBeNull:)``.
    @objc public class func column(name: String, type: BlackbirdColumnTypeObjC, mayBeNull: Bool) -> BlackbirdColumnObjC {
        BlackbirdColumnObjC(name: name, type: type, mayBeNull: mayBeNull)
    }
    
    init(name: String, type: BlackbirdColumnTypeObjC, mayBeNull: Bool) {
        self.name = name
        column = Blackbird.Column(name: name, type: type.columnType(), mayBeNull: mayBeNull)
    }
}

/// Objective-C wrapper for ``Blackbird/Index``.
@objc public class BlackbirdIndexObjC: NSObject {
    internal let index: Blackbird.Index
    
    /// Objective-C version of ``Blackbird/Index/init(columnNames:unique:)``.
    @objc public class func index(columNames: [String], unique: Bool) -> BlackbirdIndexObjC {
        BlackbirdIndexObjC(columNames: columNames, unique: unique)
    }

    init(columNames: [String], unique: Bool) {
        self.index = Blackbird.Index(columnNames: columNames, unique: unique)
    }
}

/// Objective-C wrapper for ``Blackbird/Table``.
@objc public class BlackbirdTableObjC: NSObject {
    @objc public let name: String
    @objc public let columnNames: [String]
    @objc public let primaryKeyColumnNames: [String]
    internal let table: Blackbird.Table
    
    /// Objective-C version of ``Blackbird/Table/init(name:columns:primaryKeyColumnNames:indexes:)``.
    @objc public class func table(name: String, columns: [BlackbirdColumnObjC], primaryKeyColumnNames: [String], indexes: [BlackbirdIndexObjC]) -> BlackbirdTableObjC {
        BlackbirdTableObjC(name: name, columns: columns, primaryKeyColumnNames: primaryKeyColumnNames, indexes: indexes)
    }
    
    init(name: String, columns: [BlackbirdColumnObjC], primaryKeyColumnNames: [String], indexes: [BlackbirdIndexObjC]) {
        self.name = name
        self.columnNames = columns.map { $0.name }
        self.primaryKeyColumnNames = primaryKeyColumnNames
        self.table = Blackbird.Table(name: name, columns: columns.map { $0.column }, primaryKeyColumnNames: primaryKeyColumnNames, indexes: indexes.map { $0.index })
    }
}

/// Objective-C wrapper for ``Blackbird/Database``.
@objc public class BlackbirdDatabaseObjC: NSObject {

    /// The wrapped database, accessible for use from Swift.
    public let db: Blackbird.Database

    fileprivate var cachedTableNames = Set<String>()
    
    /// Instantiates a new SQLite database as a file on disk.
    /// - Parameters:
    ///   - path: The path to the database file. If no file exists at `path`, it will be created.
    ///   - debugLogging: Whether to `print()` every query to the console.
    /// - Returns: The created instance, or `nil` if the instance could not be created at the supplied path.
    @objc public init?(path: String, debugLogging: Bool) {
        do {
            var options: Blackbird.Database.Options = [.sendLegacyChangeNotifications]
            if debugLogging { options.formUnion([.debugPrintEveryQuery, .debugPrintEveryReportedChange]) }
            db = try Blackbird.Database(path: path, options: options)
        } catch {
            print("[BlackbirdObjC] Error thrown when initializing database at [\(path)]: \(error.localizedDescription)")
            return nil
        }
    }
    
    /// Wraps an existing ``Blackbird/Database`` for use from Objective-C.
    /// - Parameter database: The ``Blackbird/Database`` to wrap.
    public init(database: Blackbird.Database) {
        db = database
    }

    /// Executes arbitrary SQL queries without returning a value.
    ///
    /// - Parameter query: The SQL string to execute. May contain multiple queries separated by semicolons (`;`).
    ///
    /// Queries are passed to SQLite without any additional parameters or automatic replacements.
    ///
    /// Any type of query valid in SQLite may be used here.
    @objc public func execute(query: String) async {
        do {
            try await db.execute(query)
        } catch {
            raiseObjCException(error)
        }
    }

    /// Synchronous version of ``execute(query:)`` using sempahores and blocking on the global queue.
    @objc public func executeSync(query: String) {
        let semaphore = DispatchSemaphore(value: 0)
        Task {
            defer { semaphore.signal() }
            do {
                try await db.execute(query)
            } catch {
                raiseObjCException(error)
            }
        }
        semaphore.wait()
    }

    /// Query the database with an optional list of arguments.
    /// - Parameters:
    ///   - query: An SQL query that may contain placeholders specified as a question mark (`?`).
    ///   - arguments: An array of values corresponding to any placeholders in the query.
    /// - Returns: An array of dictionaries matching the query if applicable, or an empty array otherwise. Each dictionary is keyed by each row's column names, and `NULL` values are represented as `NSNull.null`.
    @objc public func query(_ query: String, arguments: [Any]) async -> [[String: NSObject]] {
        do {
            return try await db.query(query, arguments).map { $0.objcRow() }
        } catch {
            raiseObjCException(error)
        }
    }
    
    /// Performs setup and any necessary schema migrations for a table.
    @objc public func resolve(table: BlackbirdTableObjC) async {
        do {
            try await db.transaction { core in
                if cachedTableNames.contains(table.name) { return }
                try table.table.resolveWithDatabaseIsolated(type: Self.self, database: db, core: core, validator: nil)
                cachedTableNames.insert(table.name)
            }
        } catch {
            raiseObjCException(error)
        }
    }
    
    /// Closes the database.
    @objc public func close() async {
        await db.close()
    }

    /// Synchronous version of ``close()`` using sempahores and blocking on the global queue.
    @objc public func closeSync() {
        let semaphore = DispatchSemaphore(value: 0)
        Task {
            await db.close()
            semaphore.signal()
        }
        semaphore.wait()
    }
}
