//
//  BlackbirdModel.swift
//  Created by Marco Arment on 11/8/22.
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

extension PartialKeyPath: @unchecked Sendable { }

/// A model protocol based on `Codable` and SQLite.
///
/// ## Defining the schema
/// Types that conform to `BlackbirdModel` must define a ``Blackbird/Table`` with database columns, indexes, and a primary key.
///
/// **Example:** A simple model:
/// ```swift
/// struct Post: BlackbirdModel {
///     @BlackbirdColumn var id: Int
///     @BlackbirdColumn var title: String
///     @BlackbirdColumn var url: URL?
/// }
/// ```
/// > If the primary key is not specified, it is assumed to be a column named `"id"`.
///
/// **Example:** A model with a custom primary-key column:
/// ```swift
/// struct CustomPrimaryKeyModel: BlackbirdModel {
///     static var primaryKey: [BlackbirdColumnKeyPath] = [ \.$pk ]
///
///     @BlackbirdColumn var pk: Int
///     @BlackbirdColumn var title: String
/// }
/// ```
///
/// **Example:** A model with indexes and a multicolumn primary key:
/// ```swift
/// struct Post: BlackbirdModel {
///     @BlackbirdColumn var id: Int
///     @BlackbirdColumn var title: String
///     @BlackbirdColumn var date: Date
///     @BlackbirdColumn var isPublished: Bool
///     @BlackbirdColumn var url: URL?
///     @BlackbirdColumn var productID: Int
///
///     static var primaryKey: [BlackbirdColumnKeyPath] = [ \.$id, \.$title ]
///
///     static var indexes: [[BlackbirdColumnKeyPath]] = [
///         [ \.$title ],
///         [ \.$isPublished, \.$date ]
///     ]
///
///     static var uniqueIndexes: [[BlackbirdColumnKeyPath]] = [
///         [ \.$productID ]
///     ]
/// }
/// ```
///
/// ### Schema migrations
///
/// If a table with the same name already exists in a database, a schema migration will be attempted for the following operations:
/// * Adding or dropping columns
/// * Adding or dropping indexes
/// * Changing column type or nullability
/// * Changing the primary key
/// If the migration fails, an error will be thrown.
///
/// Schema migrations are performed when the first `BlackbirdModel` database operation is performed for a given table.
/// To perform any necessary migrations in advance, you may optionally call ``resolveSchema(in:)-89xpw``.
///
/// ## Reading and writing
/// Most reads and writes are performed asynchronously:
/// ```swift
/// let db = try Blackbird.Database(path: "/tmp/test.sqlite")
///
/// // Write a new instance to the database
/// let post = Post(id: 1, title: "What I had for breakfast")
/// try await post.write(to: db)
///
/// // Fetch an existing instance by primary key
/// let anotherPost = try await Post.read(from: db, id: 2)
///
/// // Or with a WHERE query, parameterized with SQLite data types
/// let theSportsPost = try await Post.read(from: db, where: "title = ?", "Sports")
///
/// // Modify an instance and re-save it to the database
/// if let firstPost = try await Post.read(from: db, id: 1) {
///    var modifiedPost = firstPost
///    modifiedPost.title = "New title"
///    try await modifiedPost.write(to db)
/// }
///
/// // Or use an SQL query to modify the database directly
/// try await Post.query("UPDATE $T SET title = ? WHERE id = ?", "New title", 1)
/// ```
///
/// Synchronous access is provided in ``Blackbird/Database/transaction(_:)``.
///
/// ## Change notifications
/// When a table is modified, its ``Blackbird/ChangePublisher`` emits with the affected primary-key values:
/// * If the specific affected rows are known, their primary-key values are emitted.
/// * If an unknown set of rows or the entire table are affected, `nil` is emitted.
///
/// ```swift
/// let listener = Post.changePublisher(in: db).sink { changedPrimaryKeys in
///     print("Post IDs changed: \(changedPrimaryKeys ?? "all of them")")
/// }
/// ```
///
/// ## Unsupported SQLite features
///
/// `BlackbirdModel` assumes simple and straightforward SQLite usage.
///
/// Features such as foreign keys, virtual tables, views, autoincrement, partial or expression indexes, attached databases, etc.
/// are unsupported and untested.
///
/// While ``Blackbird/Database`` should work with a more broad set of SQL and SQLite features,
/// `BlackbirdModel` should not be used with tables or queries involving them,
/// and their use may cause some features not to behave as expected.
///
public protocol BlackbirdModel: Codable, Equatable, Identifiable, Sendable {
    typealias BlackbirdColumnKeyPath = PartialKeyPath<Self>
    
    /// The table name to use in the database. By default, the type's name is used.
    static var tableName: String { get }
    
    /// The column or columns to use as the primary key in the database table.
    ///
    /// If unspecified, the primary key is assumed to be a single column named `id`, and if no such column exists, an error is thrown.
    static var primaryKey: [BlackbirdColumnKeyPath] { get }
    
    /// An array of arrays, each specifying an index to create on the database table.
    ///
    /// The primary key is always implicitly indexed. Do not specify it as a separate index.
    ///
    /// If unspecified, no additional indexes are created.
    static var indexes: [[BlackbirdColumnKeyPath]] { get }
    
    /// An array of arrays, each specifying an index to create on the database table in which each row must have a unique value or `NULL`.
    ///
    /// The primary key is always implicitly uniquely indexed. Do not specify it as a separate index.
    ///
    /// If unspecified, no additional unique indexes are created.
    static var uniqueIndexes: [[BlackbirdColumnKeyPath]] { get }

    /// Performs setup and any necessary schema migrations.
    ///
    /// Optional. If not called manually, setup and schema migrations will occur when the first database operation is performed for the conforming type's table.
    ///
    /// If the database migration fails, an error will be thrown.
    ///
    /// - Parameters:
    ///   - database: The ``Blackbird/Database`` instance to resolve the schema in.
    static func resolveSchema(in database: Blackbird.Database) async throws

    /// Reads a single instance with the given primary-key value from a database.
    /// - Parameters:
    ///   - database: The ``Blackbird/Database`` instance to read from.
    ///   - primaryKey: The value of the primary-key column.
    /// - Returns: The decoded instance in the table with the given primary key, or `nil` if a corresponding instance doesn't exist in the table.
    ///
    /// This method is only for tables with single-column primary keys.
    ///
    /// For tables with multi-column primary keys, use ``read(from:multicolumnPrimaryKey:)-1tnpt``.
    ///
    /// For tables with a single-column primary key named `id`, ``read(from:id:)-7jbwn`` is more concise.
    static func read(from database: Blackbird.Database, primaryKey: Sendable) async throws -> Self?

    /// Synchronous version of ``read(from:primaryKey:)-7iyqj`` for use when the database actor is isolated within calls to ``Blackbird/Database/transaction(_:)`` or ``Blackbird/Database/cancellableTransaction(_:)``.
    static func readIsolated(from database: Blackbird.Database, core: isolated Blackbird.Database.Core, primaryKey: Sendable) throws -> Self?

    /// Reads a single instance with the given primary key values from a database.
    /// - Parameters:
    ///   - database: The ``Blackbird/Database`` instance to read from.
    ///   - multicolumnPrimaryKey: The array of values of the primary-key columns. Must match the number and order of primary-key values defined in the model's table.
    /// - Returns: The decoded instance in the table with the given primary key, or `nil` if a corresponding instance doesn't exist in the table.
    ///
    /// For tables with single-column primary keys, ``read(from:primaryKey:)-7iyqj`` is more concise.
    static func read(from database: Blackbird.Database, multicolumnPrimaryKey: [Sendable]) async throws -> Self?

    /// Synchronous version of ``read(from:multicolumnPrimaryKey:)-926f3`` for use when the database actor is isolated within calls to ``Blackbird/Database/transaction(_:)`` or ``Blackbird/Database/cancellableTransaction(_:)``.
    static func readIsolated(from database: Blackbird.Database, core: isolated Blackbird.Database.Core, multicolumnPrimaryKey: [Sendable]) throws -> Self?

    /// Reads a single instance with the given primary key values from a database.
    /// - Parameters:
    ///   - database: The ``Blackbird/Database`` instance to read from.
    ///   - multicolumnPrimaryKey: A dictionary of column names and values of the primary-key columns. Must match the number of primary-key values defined in the model's table.
    /// - Returns: The decoded instance in the table with the given primary key, or `nil` if a corresponding instance doesn't exist in the table.
    ///
    /// For tables with single-column primary keys, ``read(from:primaryKey:)-7ht1j`` is more concise.
    static func read(from database: Blackbird.Database, multicolumnPrimaryKey: [String: Sendable]) async throws -> Self?

    /// Synchronous version of ``read(from:multicolumnPrimaryKey:)-6pd09`` for use when the database actor is isolated within calls to ``Blackbird/Database/transaction(_:)`` or ``Blackbird/Database/cancellableTransaction(_:)``.
    static func readIsolated(from database: Blackbird.Database, core: isolated Blackbird.Database.Core, multicolumnPrimaryKey: [String: Sendable]) throws -> Self?

    /// Reads a single instance with the given primary-key value from a database if the primary key is a single column named `id`.
    /// - Parameters:
    ///   - database: The ``Blackbird/Database`` instance to read from.
    ///   - id: The value of the `id` column.
    /// - Returns: The decoded instance in the table with the given `id`, or `nil` if a corresponding instance doesn't exist in the table.
    ///
    /// For tables with other primary-key names, see ``read(from:primaryKey:)-7ht1j`` and ``read(from:multicolumnPrimaryKey:)-1tnpt``.
    static func read(from database: Blackbird.Database, id: Sendable) async throws -> Self?

    /// Synchronous version of ``read(from:id:)-9o01t`` for use when the database actor is isolated within calls to ``Blackbird/Database/transaction(_:)`` or ``Blackbird/Database/cancellableTransaction(_:)``.
    static func readIsolated(from database: Blackbird.Database, core: isolated Blackbird.Database.Core, id: Sendable) throws -> Self?

    /// Reads instances from a database using an optional list of arguments.
    ///
    /// - Parameters:
    ///   - database: The ``Blackbird/Database`` instance to read from.
    ///   - queryAfterWhere: The portion of the desired SQL query after the `WHERE` keyword. May contain placeholders specified as a question mark (`?`).
    ///   - arguments: Values corresponding to any placeholders in the query.
    /// - Returns: An array of decoded instances matching the query.
    ///
    /// ## Example
    /// ```swift
    /// let posts = try await Post.read(
    ///     from: db,
    ///     where: "state = ? OR title = ? ORDER BY time DESC",
    ///     arguments: [1 /* state */, "Test Title" /* title *]
    /// )
    /// ```
    static func read(from database: Blackbird.Database, where queryAfterWhere: String, _ arguments: Sendable...) async throws -> [Self]

    /// Synchronous version of ``read(from:where:_:)-1ja5t`` for use when the database actor is isolated within calls to ``Blackbird/Database/transaction(_:)`` or ``Blackbird/Database/cancellableTransaction(_:)``.
    static func readIsolated(from database: Blackbird.Database, core: isolated Blackbird.Database.Core, where queryAfterWhere: String, _ arguments: Sendable...) throws -> [Self]

    /// Reads instances from a database using an array of arguments.
    ///
    /// - Parameters:
    ///   - database: The ``Blackbird/Database`` instance to read from.
    ///   - queryAfterWhere: The portion of the desired SQL query after the `WHERE` keyword. May contain placeholders specified as a question mark (`?`).
    ///   - arguments: An array of values corresponding to any placeholders in the query.
    /// - Returns: An array of decoded instances matching the query.
    ///
    /// ## Example
    /// ```swift
    /// let posts = try await Post.read(
    ///     from: db,
    ///     where: "state = ? OR title = ? ORDER BY time DESC",
    ///     arguments: [1 /* state */, "Test Title" /* title *]
    /// )
    /// ```
    static func read(from database: Blackbird.Database, where queryAfterWhere: String, arguments: [Sendable]) async throws -> [Self]

    /// Synchronous version of ``read(from:where:arguments:)-5plkh`` for use when the database actor is isolated within calls to ``Blackbird/Database/transaction(_:)`` or ``Blackbird/Database/cancellableTransaction(_:)``.
    static func readIsolated(from database: Blackbird.Database, core: isolated Blackbird.Database.Core, where queryAfterWhere: String, arguments: [Sendable]) throws -> [Self]

    /// Reads instances from a database using a dictionary of named arguments.
    ///
    /// - Parameters:
    ///   - database: The ``Blackbird/Database`` instance to read from.
    ///   - queryAfterWhere: The portion of the desired SQL query after the `WHERE` keyword. May contain named placeholders prefixed by a colon (`:`), at-sign (`@`), or dollar sign (`$`) as described in the [SQLite documentation](https://www.sqlite.org/c3ref/bind_blob.html).
    ///   - arguments: A dictionary of placeholder names used in the query and their corresponding values. Names must include the prefix character used.
    /// - Returns: An array of decoded instances matching the query.
    ///
    /// ## Example
    /// ```swift
    /// let posts = try await Post.read(
    ///     from: db,
    ///     where: "state = :state OR title = :title ORDER BY time DESC",
    ///     arguments: [":state": 1, ":title": "Test Title"]
    /// )
    /// ```
    static func read(from database: Blackbird.Database, where queryAfterWhere: String, arguments: [String: Sendable]) async throws -> [Self]

    /// Synchronous version of ``read(from:where:arguments:)-31y52`` for use when the database actor is isolated within calls to ``Blackbird/Database/transaction(_:)`` or ``Blackbird/Database/cancellableTransaction(_:)``.
    static func readIsolated(from database: Blackbird.Database, core: isolated Blackbird.Database.Core, where queryAfterWhere: String, arguments: [String: Sendable]) throws -> [Self]

    /// Write this instance to a database.
    /// - Parameter database: The ``Blackbird/Database`` instance to write to.
    func write(to database: Blackbird.Database) async throws
    
    /// Write this instance to a database synchronously from an actor-isolated transaction.
    /// - Parameters:
    ///   - database: The ``Blackbird/Database`` instance to write to.
    ///   - core: The isolated ``Blackbird/Database/Core`` provided to the transaction.
    ///
    /// For use only when the database actor is isolated within calls to ``Blackbird/Database/transaction(_:)`` or ``Blackbird/Database/cancellableTransaction(_:)``.
    func writeIsolated(to database: Blackbird.Database, core: isolated Blackbird.Database.Core) throws

    /// Delete this instance from a database.
    /// - Parameter database: The ``Blackbird/Database`` instance to delete from.
    func delete(from database: Blackbird.Database) async throws
    
    /// Delete this instance from a database synchronously from an actor-isolated transaction.
    /// - Parameters:
    ///   - database: The ``Blackbird/Database`` instance to delete from.
    ///   - core: The isolated ``Blackbird/Database/Core`` provided to the transaction.
    ///
    /// For use only when the database actor is isolated within calls to ``Blackbird/Database/transaction(_:)`` or ``Blackbird/Database/cancellableTransaction(_:)``.
    func deleteIsolated(from database: Blackbird.Database, core: isolated Blackbird.Database.Core) throws


    /// Executes arbitrary SQL with a placeholder available for this type's table name.
    /// - Parameters:
    ///   - database: The ``Blackbird/Database`` instance to query.
    ///   - query: The SQL statement to execute, optionally containing:
    ///       - A `$T` placeholder which will be replaced with this type's table name.
    ///       - Question-mark placeholders (`?`) for any argument values to be passed to the query.
    ///   - arguments: Values corresponding to any placeholders in the query.
    /// - Returns: An array of rows matching the query if applicable, or an empty array otherwise.
    @discardableResult static func query(in database: Blackbird.Database, _ query: String, _ arguments: Sendable...) async throws -> [Blackbird.Row]

    /// Synchronous version of ``query(in:_:_:)-3n1pp`` for use when the database actor is isolated within calls to ``Blackbird/Database/transaction(_:)`` or ``Blackbird/Database/cancellableTransaction(_:)``.
    @discardableResult static func queryIsolated(in database: Blackbird.Database, core: isolated Blackbird.Database.Core, _ query: String, _ arguments: Sendable...) throws -> [Blackbird.Row]
    

    /// Executes arbitrary SQL with a placeholder available for this type's table name.
    /// - Parameters:
    ///   - database: The ``Blackbird/Database`` instance to query.
    ///   - query: The SQL statement to execute, optionally containing:
    ///       - A `$T` placeholder which will be replaced with this type's table name.
    ///       - Question-mark placeholders (`?`) for any argument values to be passed to the query.
    ///   - arguments: An array of values corresponding to any placeholders in the query.
    /// - Returns: An array of rows matching the query if applicable, or an empty array otherwise.
    @discardableResult static func query(in database: Blackbird.Database, _ query: String, arguments: [Sendable]) async throws -> [Blackbird.Row]

    /// Synchronous version of ``query(in:_:arguments:)-7qfll`` for use when the database actor is isolated within calls to ``Blackbird/Database/transaction(_:)`` or ``Blackbird/Database/cancellableTransaction(_:)``.
    @discardableResult static func queryIsolated(in database: Blackbird.Database, core: isolated Blackbird.Database.Core, _ query: String, arguments: [Sendable]) throws -> [Blackbird.Row]


    /// Executes arbitrary SQL with a placeholder available for this type's table name.
    /// - Parameters:
    ///   - database: The ``Blackbird/Database`` instance to query.
    ///   - query: The SQL statement to execute, optionally containing:
    ///       - A `$T` placeholder which will be replaced with this type's table name.
    ///       - Named placeholders prefixed by a colon (`:`), at-sign (`@`), or dollar sign (`$`) as described in the [SQLite documentation](https://www.sqlite.org/c3ref/bind_blob.html).
    ///   - arguments: A dictionary of placeholder names used in the query and their corresponding values. Names must include the prefix character used.
    /// - Returns: An array of rows matching the query if applicable, or an empty array otherwise.
    @discardableResult static func query(in database: Blackbird.Database, _ query: String, arguments: [String: Sendable]) async throws -> [Blackbird.Row]

    /// Synchronous version of ``query(in:_:arguments:)-6eh8j`` for use when the database actor is isolated within calls to ``Blackbird/Database/transaction(_:)`` or ``Blackbird/Database/cancellableTransaction(_:)``.
    @discardableResult static func queryIsolated(in database: Blackbird.Database, core: isolated Blackbird.Database.Core, _ query: String, arguments: [String: Sendable]) throws -> [Blackbird.Row]


    /// The change publisher for this model's table.
    /// - Parameter database: The ``Blackbird/Database`` instance to monitor.
    /// - Returns: The ``Blackbird/ChangePublisher`` for this model's table.
    static func changePublisher(in database: Blackbird.Database) -> Blackbird.ChangePublisher

    /// Shorthand for this type's `ModelArrayUpdater` interface for SwiftUI.
    typealias ArrayUpdater = Blackbird.ModelArrayUpdater<Self>
    
    /// Shorthand for this type's `ModelInstanceUpdater` interface for SwiftUI.
    typealias InstanceUpdater = Blackbird.ModelInstanceUpdater<Self>
}

internal extension BlackbirdModel {
    static var table: Blackbird.Table { SchemaGenerator.shared.table(for: Self.self) }
}

extension BlackbirdModel {
    public static var tableName: String { String(describing: Self.self) }
    public static var primaryKey: [BlackbirdColumnKeyPath] { [] }
    public static var indexes: [[BlackbirdColumnKeyPath]] { [] }
    public static var uniqueIndexes: [[BlackbirdColumnKeyPath]] { [] }
        
    // Identifiable
    public var id: [AnyHashable] {
        Self.primaryKey.map {
            guard let wrapper = self[keyPath: $0] as? any ColumnWrapper else { fatalError("Cannot access @BlackbirdColumn wrapper from primaryKey") }
            return AnyHashable(wrapper.value)
        }
    }
    
    public static func changePublisher(in database: Blackbird.Database) -> Blackbird.ChangePublisher { database.changeReporter.changePublisher(for: self.tableName) }
    
    public static func read(from database: Blackbird.Database, id: Sendable) async throws -> Self? { return try await self.read(from: database, where: "id = ?", id).first }

    public static func readIsolated(from database: Blackbird.Database, core: isolated Blackbird.Database.Core, id: Sendable) throws -> Self? { return try self.readIsolated(from: database, core: core, where: "id = ?", id).first }

    public static func read(from database: Blackbird.Database, primaryKey: Sendable) async throws -> Self? { return try await self.read(from: database, multicolumnPrimaryKey: [primaryKey]) }

    public static func readIsolated(from database: Blackbird.Database, core: isolated Blackbird.Database.Core, primaryKey: Sendable) throws -> Self? { return try self.readIsolated(from: database, core: core, multicolumnPrimaryKey: [primaryKey]) }

    public static func read(from database: Blackbird.Database, multicolumnPrimaryKey: [Sendable]) async throws -> Self? {
        if multicolumnPrimaryKey.count != table.primaryKeys.count {
            fatalError("Incorrect number of primary-key values provided (\(multicolumnPrimaryKey.count), need \(table.primaryKeys.count)) for table \(tableName)")
        }
        return try await self.read(from: database, where: table.primaryKeys.map { "`\($0.name)` = ?" }.joined(separator: " AND "), multicolumnPrimaryKey).first
    }

    public static func readIsolated(from database: Blackbird.Database, core: isolated Blackbird.Database.Core, multicolumnPrimaryKey: [Sendable]) throws -> Self? {
        if multicolumnPrimaryKey.count != table.primaryKeys.count {
            fatalError("Incorrect number of primary-key values provided (\(multicolumnPrimaryKey.count), need \(table.primaryKeys.count)) for table \(tableName)")
        }
        return try self.readIsolated(from: database, core: core, where: table.primaryKeys.map { "`\($0.name)` = ?" }.joined(separator: " AND "), multicolumnPrimaryKey).first
    }

    public static func read(from database: Blackbird.Database, multicolumnPrimaryKey: [String: Sendable]) async throws -> Self? {
        if multicolumnPrimaryKey.count != table.primaryKeys.count {
            fatalError("Incorrect number of primary-key values provided (\(multicolumnPrimaryKey.count), need \(table.primaryKeys.count)) for table \(tableName)")
        }
        
        var andClauses: [String] = []
        var values: [Sendable] = []
        for (name, value) in multicolumnPrimaryKey {
            andClauses.append("`\(name)` = ?")
            values.append(value)
        }
        
        return try await self.read(from: database, where: andClauses.joined(separator: " AND "), arguments: values).first
    }

    public static func readIsolated(from database: Blackbird.Database, core: isolated Blackbird.Database.Core, multicolumnPrimaryKey: [String: Sendable]) throws -> Self? {
        if multicolumnPrimaryKey.count != table.primaryKeys.count {
            fatalError("Incorrect number of primary-key values provided (\(multicolumnPrimaryKey.count), need \(table.primaryKeys.count)) for table \(tableName)")
        }
        
        var andClauses: [String] = []
        var values: [Sendable] = []
        for (name, value) in multicolumnPrimaryKey {
            andClauses.append("`\(name)` = ?")
            values.append(value)
        }
        
        return try self.readIsolated(from: database, core: core, where: andClauses.joined(separator: " AND "), arguments: values).first
    }

    public static func read(from database: Blackbird.Database, where queryAfterWhere: String, _ arguments: Sendable...) async throws -> [Self] {
        return try await self.read(from: database, where: queryAfterWhere, arguments: arguments)
    }

    public static func read(from database: Blackbird.Database, where queryAfterWhere: String, arguments: [Sendable]) async throws -> [Self] {
        return try await query(in: database, "SELECT * FROM $T WHERE \(queryAfterWhere)", arguments: arguments).map {
            let decoder = BlackbirdSQLiteDecoder($0)
            return try Self(from: decoder)
        }
    }

    public static func read(from database: Blackbird.Database, where queryAfterWhere: String, arguments: [String: Sendable]) async throws -> [Self] {
        return try await query(in: database, "SELECT * FROM $T WHERE \(queryAfterWhere)", arguments: arguments).map {
            let decoder = BlackbirdSQLiteDecoder($0)
            return try Self(from: decoder)
        }
    }

    public static func readIsolated(from database: Blackbird.Database, core: isolated Blackbird.Database.Core, where queryAfterWhere: String, _ arguments: Sendable...) throws -> [Self] {
        return try self.readIsolated(from: database, core: core, where: queryAfterWhere, arguments: arguments)
    }

    public static func readIsolated(from database: Blackbird.Database, core: isolated Blackbird.Database.Core, where queryAfterWhere: String, arguments: [Sendable]) throws -> [Self] {
        return try queryIsolated(in: database, core: core, "SELECT * FROM $T WHERE \(queryAfterWhere)", arguments: arguments).map {
            let decoder = BlackbirdSQLiteDecoder($0)
            return try Self(from: decoder)
        }
    }

    public static func readIsolated(from database: Blackbird.Database, core: isolated Blackbird.Database.Core, where queryAfterWhere: String, arguments: [String: Sendable]) throws -> [Self] {
        return try queryIsolated(in: database, core: core, "SELECT * FROM $T WHERE \(queryAfterWhere)", arguments: arguments).map {
            let decoder = BlackbirdSQLiteDecoder($0)
            return try Self(from: decoder)
        }
    }

    @discardableResult
    public static func query(in database: Blackbird.Database, _ query: String, _ arguments: Sendable...) async throws -> [Blackbird.Row] {
        return try await self.query(in: database, query, arguments: arguments)
    }

    @discardableResult
    public static func query(in database: Blackbird.Database, _ query: String, arguments: [Sendable]) async throws -> [Blackbird.Row] {
        try await table.resolveWithDatabase(type: Self.self, database: database, core: database.core) { try validateSchema() }
        return try await database.core.query(query.replacingOccurrences(of: "$T", with: tableName), arguments: arguments)
    }

    @discardableResult
    public static func query(in database: Blackbird.Database, _ query: String, arguments: [String: Sendable]) async throws -> [Blackbird.Row] {
        try await table.resolveWithDatabase(type: Self.self, database: database, core: database.core) { try validateSchema() }
        return try await database.core.query(query.replacingOccurrences(of: "$T", with: tableName), arguments: arguments)
    }

    @discardableResult
    public static func queryIsolated(in database: Blackbird.Database, core: isolated Blackbird.Database.Core, _ query: String, _ arguments: Sendable...) throws -> [Blackbird.Row] {
        return try self.queryIsolated(in: database, core: core, query, arguments: arguments)
    }

    @discardableResult
    public static func queryIsolated(in database: Blackbird.Database, core: isolated Blackbird.Database.Core, _ query: String, arguments: [Sendable]) throws -> [Blackbird.Row] {
        let table = Self.table
        try table.resolveWithDatabaseIsolated(type: Self.self, database: database, core: core) { try Self.validateSchema() }
        return try core.query(query.replacingOccurrences(of: "$T", with: tableName), arguments: arguments)
    }

    @discardableResult
    public static func queryIsolated(in database: Blackbird.Database, core: isolated Blackbird.Database.Core, _ query: String, arguments: [String: Sendable]) throws -> [Blackbird.Row] {
        try table.resolveWithDatabaseIsolated(type: Self.self, database: database, core: core) { try validateSchema() }
        return try core.query(query.replacingOccurrences(of: "$T", with: tableName), arguments: arguments)
    }

    private static func validateSchema() throws -> Void {
        var testRow = Blackbird.Row()
        for column in table.columns { testRow[column.name] = column.mayBeNull ? .null : column.type.defaultValue() }
        let decoder = BlackbirdSQLiteDecoder(testRow)
        do {
            _ = try Self(from: decoder)
        } catch {
            fatalError("Table \"\(tableName)\" definition defaults do not decode to model \(String(describing: self)): \(error)")
        }
    }
    
    public static func resolveSchema(in database: Blackbird.Database) async throws {
        try await table.resolveWithDatabase(type: Self.self, database: database, core: database.core) { try validateSchema() }
    }
    
    private func insertQueryValues() throws -> (sql: String, values: [Sendable], primaryKeyValues: [Blackbird.Value]?) {
        let table = Self.table

        let encoder = BlackbirdSQLiteEncoder()
        try self.encode(to: encoder)
        let encodedValues = encoder.sqliteArguments
        let primaryKeyValues = table.primaryKeys.map { encodedValues[$0.name]! }
        
        var columnNames: [String] = []
        var placeholders: [String] = []
        var values: [Blackbird.Value] = []
        for (key, value) in encodedValues.filter({ table.columnNames.contains($0.key) }) {
            columnNames.append(key)
            placeholders.append("?")
            values.append(value)
        }

        let sql = "REPLACE INTO `\(table.name)` (`\(columnNames.joined(separator: "`,`"))`) VALUES (\(placeholders.joined(separator: ",")))"
        return (sql: sql, values: values, primaryKeyValues: primaryKeyValues)
    }
    
    public func write(to database: Blackbird.Database) async throws {
        try await writeIsolated(to: database, core: database.core)
    }
    
    public func writeIsolated(to database: Blackbird.Database, core: isolated Blackbird.Database.Core) throws {
        if database.options.contains(.readOnly) { fatalError("Cannot write BlackbirdModel to a read-only database") }
        try Self.table.resolveWithDatabaseIsolated(type: Self.self, database: database, core: core) { try Self.validateSchema() }

        let (sql, values, primaryKeyValues) = try insertQueryValues()
        database.changeReporter.ignoreWritesToTable(Self.tableName)
        defer {
            database.changeReporter.stopIgnoringWrites()
            database.changeReporter.reportChange(tableName: Self.tableName, primaryKey: primaryKeyValues)
        }
        try core.query(sql, arguments: values)
    }

    public func delete(from database: Blackbird.Database) async throws {
        try await deleteIsolated(from: database, core: database.core)
    }

    public func deleteIsolated(from database: Blackbird.Database, core: isolated Blackbird.Database.Core) throws {
        if database.options.contains(.readOnly) { fatalError("Cannot delete BlackbirdModel from a read-only database") }
        let table = Self.table
        try table.resolveWithDatabaseIsolated(type: Self.self, database: database, core: core) { try Self.validateSchema() }

        let encoder = BlackbirdSQLiteEncoder()
        try self.encode(to: encoder)
        let sqliteColumnValues = encoder.sqliteArguments

        var andClauses: [String] = []
        var values: [Blackbird.Value] = []
        for column in table.primaryKeys {
            andClauses.append("`\(column.name)` = ?")
            let value = sqliteColumnValues[column.name]!
            values.append(value)
        }
        
        database.changeReporter.ignoreWritesToTable(Self.tableName)
        defer {
            database.changeReporter.stopIgnoringWrites()
            database.changeReporter.reportChange(tableName: Self.tableName, primaryKey: values)
        }
        let sql = "DELETE FROM `\(Self.tableName)` WHERE \(andClauses.joined(separator: " AND "))"
        try core.query(sql, arguments: values)
    }

}
