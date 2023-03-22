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

/// A model protocol based on `Codable` and SQLite.
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
/// To perform any necessary migrations in advance, you may optionally call ``resolveSchema(in:)``.
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
/// let p = try await Post.read(from: db, id: 2)
///
/// // Or with a WHERE query, parameterized with SQLite data types
/// let p = try await Post.read(from: db, sqlWhere: "title = ?", "Sports")
///
/// // Or with a WHERE query, using checked key paths as column names
/// let p = try await Post.read(from: db, sqlWhere: "\(\Post.$title) = ?", "Sports")
///
/// // Or entirely with checked key paths
/// let p = try await Post.read(from: db, matching \.$title == "Sports")
///
/// // Modify an instance and re-save it to the database
/// if let firstPost = try await Post.read(from: db, id: 1) {
///    var modifiedPost = firstPost
///    modifiedPost.title = "New title"
///    try await modifiedPost.write(to db)
/// }
///
/// // Or use an SQL query to modify the database directly
/// try await Post.query(in: db, "UPDATE $T SET title = ? WHERE id = ?", "New title", 1)
///
/// // Or use checked key paths for the column names
/// try await Post.query(in: db, "UPDATE $T SET \(\Post.$title) = ? WHERE \(\Post.$id) = ?", "New title", 1)
///
/// // Or entirely with checked key paths
/// try await Post.update(in: db, set: [ $title : "New title" ], matching: \.$id == 1)
/// ```
///
/// Synchronous access is provided in ``Blackbird/Database/transaction(_:)``.
///
/// ## Change notifications
/// When a table is modified, its ``Blackbird/ChangePublisher`` emits a ``Blackbird/Change`` specifying which primary-key values and columns have changed:
/// ```swift
/// let listener = Post.changePublisher(in: db).sink { change in
///     print("Post IDs changed: \(change.primaryKeys ?? "all of them")")
/// }
/// ```
///
/// These can be automatically filtered with ``BlackbirdModel/changePublisher(in:primaryKey:columns:)`` to specific primary-key values and/or columns that may have changed:
///
/// ```swift
/// let listener = Post.changePublisher(in: db, primaryKey: 3, columns: [\.$title]).sink { _ in
///     print("Post 3 changed its title")
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
public protocol BlackbirdModel: Codable, Equatable, Identifiable, Hashable, Sendable {
    /// A key-path to any `@BlackbirdColumn`-wrapped variable of this type, e.g. `\.$id` or `\.$title`.
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

    /// Use basic, automatic caching for primary-key reads. Disabled by default.
    ///
    /// May break assumptions if cacheable data is read by another thread during a transaction.
    ///
    /// Only works for tables with single-column primary keys.
    ///
    /// > Note: The cache is not limited in size. Use caution with memory usage if enabled for large tables.
    static var enableCaching: Bool { get }

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
    public static var enableCaching: Bool { false }

    // Identifiable
    public var id: [AnyHashable] {
        let primaryKeyPaths = Self.primaryKey
        if primaryKeyPaths.count > 0 {
            return primaryKeyPaths.map {
                guard let wrapper = self[keyPath: $0] as? any ColumnWrapper else { fatalError("Cannot access @BlackbirdColumn wrapper from primaryKey") }
                return AnyHashable(wrapper.value)
            }
        } else {
            let mirror = Mirror(reflecting: self)
            for child in mirror.children {
                if child.label == "id", let value = child.value as? any Hashable {
                    return [AnyHashable(value)]
                }
            }
            fatalError("\(String(describing: Self.self)): Cannot detect primary-key value for Identifiable. Specify a primaryKey.")
        }
    }
    
    // Hashable
    public func hash(into hasher: inout Hasher) { hasher.combine(id) }

    /// Look up ``Blackbird/ColumnInfo`` instances from key-paths to `@BlackbirdColumn` variables.
    public static func columnInfoFromKeyPaths(_ keyPaths: [PartialKeyPath<Self>]) -> [PartialKeyPath<Self>: Blackbird.ColumnInfo] {
        let table = Self.table
        var infos: [PartialKeyPath<Self>: Blackbird.ColumnInfo] = [:]
        for keyPath in keyPaths {
            infos[keyPath] = table.keyPathToColumnInfo(keyPath: keyPath)
        }
        return infos
    }

    /// The set of column names, as strings, that have changed since its last save to the specified database.
    ///
    /// This function errs toward over-reporting. If the instance was created by other means and was not read from a database, or it was read from a different database, it will return the names of all columns.
    public func changedColumns(in database: Blackbird.Database) -> Blackbird.ColumnNames {
        Blackbird.ColumnNames(Mirror(reflecting: self).children.compactMap { (label: String?, value: Any) in
            guard let column = value as? any ColumnWrapper, column.hasChanged(in: database) else { return nil }
            return label?.removingLeadingUnderscore()
        })
    }

    /// Creates a new instance of the called model type with all values set to their SQLite defaults: nil for optionals, 0 for numeric types, empty string for string values, and empty data for data values.
    public static func instanceFromDefaults() -> Self { SchemaGenerator.instanceFromDefaults(Self.self) }

    /// The change publisher for this model's table.
    /// - Parameter database: The ``Blackbird/Database`` instance to monitor.
    /// - Returns: The ``Blackbird/ChangePublisher`` for this model's table.
    ///
    /// See ``BlackbirdModel/changePublisher(in:primaryKey:columns:)`` for built-in filtering by primary-key and/or changed columns.
    ///
    /// > Note: Changes may be over-reported if the database changes externally or in response to certain queries.
    public static func changePublisher(in database: Blackbird.Database) -> Blackbird.ChangePublisher { database.changeReporter.changePublisher(for: self.tableName) }

    /// The change publisher for this model's table, filtered by single-column primary key and/or changed columns.
    /// - Parameters:
    ///   - database: The ``Blackbird/Database`` instance to monitor.
    ///   - primaryKey: The single-column primary-key value set to monitor. If `nil`, changes to any keys are reported.
    ///   - columns: Specific columns to monitor. If empty, changes to any column(s) are reported.
    /// - Returns: The filtered ``Blackbird/ChangePublisher``.
    ///
    /// Use ``BlackbirdModel/changePublisher(in:multicolumnPrimaryKey:columns:)`` for models with multi-column primary keys.
    ///
    /// > Note: Changes may be over-reported if the database changes externally or in response to certain queries.
    public static func changePublisher(in database: Blackbird.Database, primaryKey: Blackbird.Value? = nil, columns: [Self.BlackbirdColumnKeyPath] = []) -> Blackbird.ChangePublisher {
        if primaryKey != nil, table.primaryKeys.count > 1 { fatalError("\(String(describing: Self.self)).changePublisher: Single-column primary key value specified on table with a multi-column primary key") }
        
        return database.changeReporter.changePublisher(for: self.tableName).filter { change in
            if let primaryKey, let changedKeys = change.primaryKeys, !changedKeys.contains([primaryKey]) { return false }
            
            if !columns.isEmpty, let changedColumns = change.columnNames {
                let columnNames = Blackbird.ColumnNames(columns.map { table.keyPathToColumnName(keyPath: $0) })
                if columnNames.isDisjoint(with: changedColumns) { return false }
            }

            return true
        }
        .eraseToAnyPublisher()
    }

    /// The change publisher for this model's table, filtered by multi-column primary key and/or changed columns.
    /// - Parameters:
    ///   - database: The ``Blackbird/Database`` instance to monitor.
    ///   - multicolumnPrimaryKey: The multi-column primary-key value set to monitor. If `nil`, changes to any keys are reported.
    ///   - columns: Specific columns to monitor. If empty, changes to any column(s) are reported.
    /// - Returns: The filtered ``Blackbird/ChangePublisher``.
    ///
    /// Use ``BlackbirdModel/changePublisher(in:primaryKey:columns:)`` for models with single-column primary keys.
    ///
    /// > Note: Changes may be over-reported if the database changes externally or in response to certain queries.
    public static func changePublisher(in database: Blackbird.Database, multicolumnPrimaryKey: [Blackbird.Value]?, columns: [Self.BlackbirdColumnKeyPath] = []) -> Blackbird.ChangePublisher {
        database.changeReporter.changePublisher(for: self.tableName).filter { change in
            if let multicolumnPrimaryKey, let changedKeys = change.primaryKeys, !changedKeys.contains(multicolumnPrimaryKey) { return false }
            
            if !columns.isEmpty, let changedColumns = change.columnNames {
                let columnNames = Blackbird.ColumnNames(columns.map { table.keyPathToColumnName(keyPath: $0) })
                if columnNames.isDisjoint(with: changedColumns) { return false }
            }

            return true
        }
        .eraseToAnyPublisher()
    }

    /// Reads a single instance with the given primary-key value from a database if the primary key is a single column named `id`.
    /// - Parameters:
    ///   - database: The ``Blackbird/Database`` instance to read from.
    ///   - id: The value of the `id` column.
    /// - Returns: The first decoded instance in the table with the given `id`, or `nil` if a corresponding instance doesn't exist in the table.
    ///
    /// For tables with other primary-key names, see ``read(from:primaryKey:)`` and ``read(from:multicolumnPrimaryKey:)-926f3``.
    public static func read(from database: Blackbird.Database, id: Sendable) async throws -> Self? {
        let primaryKeyPaths = self.primaryKey
        if let firstKeyPath = primaryKeyPaths.first, table.keyPathToColumnName(keyPath: firstKeyPath) != "id" || primaryKeyPaths.count > 1 {
            fatalError("read(from:id:) requires that the primary key be only \"id\"")
        }
        
        let idValue = try Blackbird.Value.fromAny(id)
        if let cached = _cachedInstance(for: database, primaryKeyValue: idValue) { return cached }
        return try await self._readInternal(from: database, query: "SELECT * FROM $T WHERE id = ?", arguments: [idValue]).first
    }

    /// Synchronous version of ``read(from:id:)`` for use when the database actor is isolated within calls to ``Blackbird/Database/transaction(_:)`` or ``Blackbird/Database/cancellableTransaction(_:)``.
    public static func readIsolated(from database: Blackbird.Database, core: isolated Blackbird.Database.Core, id: Sendable) throws -> Self? {
        let primaryKeyPaths = self.primaryKey
        if let firstKeyPath = primaryKeyPaths.first, table.keyPathToColumnName(keyPath: firstKeyPath) != "id" || primaryKeyPaths.count > 1 {
            fatalError("read(from:id:) requires that the primary key be only \"id\"")
        }

        let idValue = try Blackbird.Value.fromAny(id)
        if let cached = _cachedInstance(for: database, primaryKeyValue: idValue) { return cached }
        return try self._readInternalIsolated(from: database, core: core, query: "SELECT * FROM $T WHERE id = ?", arguments: [idValue]).first
    }

    /// Reads a single instance with the given primary-key value from a database.
    /// - Parameters:
    ///   - database: The ``Blackbird/Database`` instance to read from.
    ///   - primaryKey: The value of the primary-key column.
    /// - Returns: The decoded instance in the table with the given primary key, or `nil` if a corresponding instance doesn't exist in the table.
    ///
    /// This method is only for tables with single-column primary keys.
    ///
    /// For tables with multi-column primary keys, use ``read(from:multicolumnPrimaryKey:)-926f3``.
    ///
    /// For tables with a single-column primary key named `id`, ``read(from:id:)`` is more concise.
    public static func read(from database: Blackbird.Database, primaryKey: Sendable) async throws -> Self? { return try await self.read(from: database, multicolumnPrimaryKey: [primaryKey]) }

    /// Synchronous version of ``read(from:primaryKey:)`` for use when the database actor is isolated within calls to ``Blackbird/Database/transaction(_:)`` or ``Blackbird/Database/cancellableTransaction(_:)``.
    public static func readIsolated(from database: Blackbird.Database, core: isolated Blackbird.Database.Core, primaryKey: Sendable) throws -> Self? { return try self.readIsolated(from: database, core: core, multicolumnPrimaryKey: [primaryKey]) }


    // SQLite limits the number of "?" arguments in a query. This splits the given values into chunks that fit well below that limit.
    private static func _queryVariableLimitChunks(for database: Blackbird.Database, _ values: [Sendable]) -> [[Sendable]] {
        let chunkSize = database.maxQueryVariableCount / 2
        let count = values.count
        
        // Thanks, Paul Hudson: https://www.hackingwithswift.com/example-code/language/how-to-split-an-array-into-chunks
        return stride(from: 0, to: count, by: chunkSize).map {
            Array(values[$0 ..< Swift.min($0 + chunkSize, count)])
        }
    }

    private static func _sortWithPrimaryKeyValueSequence(instances: [Self], primaryKeyValues: [Blackbird.Value]) -> [Self] {
        var primaryKeyValuesToInstances: [Blackbird.Value : Self] = [:]
        for instance in instances {
            primaryKeyValuesToInstances[try! Blackbird.Value.fromAny(instance.primaryKeyValues().first!)] = instance
        }
        return primaryKeyValues.compactMap { primaryKeyValuesToInstances[$0] }
    }

    /// Reads an array of instances matching the given primary-key values from a database. Only works with single-column primary keys.
    /// - Parameters:
    ///   - database: The ``Blackbird/Database`` instance to read from.
    ///   - primaryKeys: The values of the primary-key column.
    ///   - preserveOrder: Sort the results to match the order of the `primaryKeys` parameter. Incurs a performance penalty on large collections. Default: `false`.
    /// - Returns: Any decoded instances in the table matching the given set of primary keys if they exist.
    ///
    /// Equivalent to the SQL `IN` clause, e.g.:
    /// 
    /// `SELECT ... WHERE [primary key column] IN (?, ?, ?, ...)`
    public static func read(from database: Blackbird.Database, primaryKeys: [Sendable], preserveOrder: Bool = false) async throws -> [Self] {
        let pkName = table.primaryKeys.first!.name
        let primaryKeys = try primaryKeys.map { try Blackbird.Value.fromAny($0) }
        let cacheResult = _cachedInstances(for: database, primaryKeyValues: primaryKeys)
        var combinedResults: [Self] = cacheResult.hits
        
        for primaryKeyChunk in _queryVariableLimitChunks(for: database, cacheResult.missedKeys) {
            let placeholderStr = Array(repeating: "?", count: primaryKeyChunk.count).joined(separator: ",")
            let resultsChunk = try await self.read(from: database, sqlWhere: "\(pkName) IN (\(placeholderStr))", primaryKeyChunk)
            combinedResults.append(contentsOf: resultsChunk)
        }
        return preserveOrder ? _sortWithPrimaryKeyValueSequence(instances: combinedResults, primaryKeyValues: primaryKeys) : combinedResults
    }

    /// Synchronous version of ``read(from:primaryKeys:preserveOrder:)`` for use when the database actor is isolated within calls to ``Blackbird/Database/transaction(_:)`` or ``Blackbird/Database/cancellableTransaction(_:)``.
    public static func readIsolated(from database: Blackbird.Database, core: isolated Blackbird.Database.Core, primaryKeys: [Sendable], preserveOrder: Bool = false) throws -> [Self] {
        let pkName = table.primaryKeys.first!.name
        let primaryKeys = try primaryKeys.map { try Blackbird.Value.fromAny($0) }
        let cacheResult = _cachedInstances(for: database, primaryKeyValues: primaryKeys)
        var combinedResults: [Self] = cacheResult.hits
        
        for primaryKeyChunk in _queryVariableLimitChunks(for: database, cacheResult.missedKeys) {
            let placeholderStr = Array(repeating: "?", count: primaryKeyChunk.count).joined(separator: ",")
            let resultsChunk = try self.readIsolated(from: database, core: core, sqlWhere: "\(pkName) IN (\(placeholderStr))", primaryKeyChunk)
            combinedResults.append(contentsOf: resultsChunk)
        }
        return preserveOrder ? _sortWithPrimaryKeyValueSequence(instances: combinedResults, primaryKeyValues: primaryKeys) : combinedResults
    }

    /// Reads a single instance with the given primary key values from a database.
    /// - Parameters:
    ///   - database: The ``Blackbird/Database`` instance to read from.
    ///   - multicolumnPrimaryKey: An array of values of the primary-key columns. Must match the number and order of primary-key values defined in the model's table.
    /// - Returns: The decoded instance in the table with the given primary key, or `nil` if a corresponding instance doesn't exist in the table.
    ///
    /// For tables with single-column primary keys, ``read(from:primaryKey:)`` is more concise.
    public static func read(from database: Blackbird.Database, multicolumnPrimaryKey: [Sendable]) async throws -> Self? {
        if multicolumnPrimaryKey.count != table.primaryKeys.count {
            fatalError("Incorrect number of primary-key values provided (\(multicolumnPrimaryKey.count), need \(table.primaryKeys.count)) for table \(tableName)")
        }
        return try await self.read(from: database, sqlWhere: table.primaryKeys.map { "`\($0.name)` = ?" }.joined(separator: " AND "), multicolumnPrimaryKey).first
    }

    /// Synchronous version of ``read(from:multicolumnPrimaryKey:)-926f3`` for use when the database actor is isolated within calls to ``Blackbird/Database/transaction(_:)`` or ``Blackbird/Database/cancellableTransaction(_:)``.
    public static func readIsolated(from database: Blackbird.Database, core: isolated Blackbird.Database.Core, multicolumnPrimaryKey: [Sendable]) throws -> Self? {
        if multicolumnPrimaryKey.count != table.primaryKeys.count {
            fatalError("Incorrect number of primary-key values provided (\(multicolumnPrimaryKey.count), need \(table.primaryKeys.count)) for table \(tableName)")
        }
        return try self.readIsolated(from: database, core: core, sqlWhere: table.primaryKeys.map { "`\($0.name)` = ?" }.joined(separator: " AND "), multicolumnPrimaryKey).first
    }

    /// Reads a single instance with the given primary key values from a database.
    /// - Parameters:
    ///   - database: The ``Blackbird/Database`` instance to read from.
    ///   - multicolumnPrimaryKey: A dictionary of column names and values of the primary-key columns. Must match the number of primary-key values defined in the model's table.
    /// - Returns: The decoded instance in the table with the given primary key, or `nil` if a corresponding instance doesn't exist in the table.
    ///
    /// For tables with single-column primary keys, ``read(from:primaryKey:)`` is more concise.
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
        
        return try await self.read(from: database, sqlWhere: andClauses.joined(separator: " AND "), arguments: values).first
    }

    /// Synchronous version of ``read(from:multicolumnPrimaryKey:)-6pd09`` for use when the database actor is isolated within calls to ``Blackbird/Database/transaction(_:)`` or ``Blackbird/Database/cancellableTransaction(_:)``.
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
        
        return try self.readIsolated(from: database, core: core, sqlWhere: andClauses.joined(separator: " AND "), arguments: values).first
    }

    /// Reads instances from a database using an optional list of arguments.
    ///
    /// - Parameters:
    ///   - database: The ``Blackbird/Database`` instance to read from.
    ///   - sqlWhere: The portion of the desired SQL query after the `WHERE` keyword. May contain placeholders specified as a question mark (`?`).
    ///   - arguments: Values corresponding to any placeholders in the query.
    /// - Returns: An array of decoded instances matching the query.
    ///
    /// ## Example
    /// ```swift
    /// let posts = try await Post.read(
    ///     from: db,
    ///     sqlWhere: "state = ? OR title = ? ORDER BY time DESC",
    ///     arguments: [1 /* state */, "Test Title" /* title *]
    /// )
    /// ```
    public static func read(from database: Blackbird.Database, sqlWhere: String, _ arguments: Sendable...) async throws -> [Self] {
        return try await self.read(from: database, sqlWhere: sqlWhere, arguments: arguments)
    }

    /// Reads instances from a database using an array of arguments.
    ///
    /// - Parameters:
    ///   - database: The ``Blackbird/Database`` instance to read from.
    ///   - sqlWhere: The portion of the desired SQL query after the `WHERE` keyword. May contain placeholders specified as a question mark (`?`).
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
    public static func read(from database: Blackbird.Database, sqlWhere: String, arguments: [Sendable]) async throws -> [Self] {
        let arguments = try arguments.map { try Blackbird.Value.fromAny($0) }
        let query = "SELECT * FROM $T WHERE \(sqlWhere)"
        return try await _cacheableResult(database: database, tableName: self.tableName, query: query, arguments: arguments) {
            try await _readInternal(from: $0, query: query, arguments: arguments)
        }
    }

    internal static func _readInternal(from database: Blackbird.Database, query: String, arguments: [Sendable]) async throws -> [Self] {
        let arguments = try arguments.map { try Blackbird.Value.fromAny($0) }
        return try await self._queryInternal(in: database, query, arguments: arguments).map {
            let decoder = BlackbirdSQLiteDecoder(database: database, row: $0.row)
            let instance = try Self(from: decoder)
            if Self.enableCaching { instance._saveCachedInstance(for: database) }
            return instance
        }
    }

    internal static func _readInternalIsolated(from database: Blackbird.Database, core: isolated Blackbird.Database.Core, query: String, arguments: [Sendable]) throws -> [Self] {
        let arguments = try arguments.map { try Blackbird.Value.fromAny($0) }
        return try self._queryInternalIsolated(in: database, core: core, query, arguments: arguments).map {
            let decoder = BlackbirdSQLiteDecoder(database: database, row: $0.row)
            let instance = try Self(from: decoder)
            if Self.enableCaching { instance._saveCachedInstance(for: database) }
            return instance
        }
    }

    /// Reads instances from a database using a dictionary of named arguments.
    ///
    /// - Parameters:
    ///   - database: The ``Blackbird/Database`` instance to read from.
    ///   - sqlWhere: The portion of the desired SQL query after the `WHERE` keyword. May contain named placeholders prefixed by a colon (`:`), at-sign (`@`), or dollar sign (`$`) as described in the [SQLite documentation](https://www.sqlite.org/c3ref/bind_blob.html).
    ///   - arguments: A dictionary of placeholder names used in the query and their corresponding values. Names must include the prefix character used.
    /// - Returns: An array of decoded instances matching the query.
    ///
    /// ## Example
    /// ```swift
    /// let posts = try await Post.read(
    ///     from: db,
    ///     sqlWhere: "state = :state OR title = :title ORDER BY time DESC",
    ///     arguments: [":state": 1, ":title": "Test Title"]
    /// )
    /// ```
    public static func read(from database: Blackbird.Database, sqlWhere: String, arguments: [String: Sendable]) async throws -> [Self] {
        return try await query(in: database, "SELECT * FROM $T WHERE \(sqlWhere)", arguments: arguments).map {
            let decoder = BlackbirdSQLiteDecoder(database: database, row: $0.row)
            let instance = try Self(from: decoder)
            if Self.enableCaching { instance._saveCachedInstance(for: database) }
            return instance
        }
    }

    /// Synchronous version of ``read(from:sqlWhere:_:)`` for use when the database actor is isolated within calls to ``Blackbird/Database/transaction(_:)`` or ``Blackbird/Database/cancellableTransaction(_:)``.
    public static func readIsolated(from database: Blackbird.Database, core: isolated Blackbird.Database.Core, sqlWhere: String, _ arguments: Sendable...) throws -> [Self] {
        return try self.readIsolated(from: database, core: core, sqlWhere: sqlWhere, arguments: arguments)
    }

    /// Synchronous version of ``read(from:sqlWhere:arguments:)-1cd9m`` for use when the database actor is isolated within calls to ``Blackbird/Database/transaction(_:)`` or ``Blackbird/Database/cancellableTransaction(_:)``.
    public static func readIsolated(from database: Blackbird.Database, core: isolated Blackbird.Database.Core, sqlWhere: String, arguments: [Sendable]) throws -> [Self] {
        let query = "SELECT * FROM $T WHERE \(sqlWhere)"
        let arguments = try arguments.map { try Blackbird.Value.fromAny($0) }
        return try _cacheableResultIsolated(database: database, core: core, tableName: self.tableName, query: query, arguments: arguments) {
            return try self._queryInternalIsolated(in: $0, core: $1, query, arguments: arguments).map {
                let decoder = BlackbirdSQLiteDecoder(database: database, row: $0.row)
                let instance = try Self(from: decoder)
                if Self.enableCaching { instance._saveCachedInstance(for: database) }
                return instance
            }
        }
    }

    /// Synchronous version of ``read(from:sqlWhere:arguments:)-5y16m`` for use when the database actor is isolated within calls to ``Blackbird/Database/transaction(_:)`` or ``Blackbird/Database/cancellableTransaction(_:)``.
    public static func readIsolated(from database: Blackbird.Database, core: isolated Blackbird.Database.Core, sqlWhere: String, arguments: [String: Sendable]) throws -> [Self] {
        return try queryIsolated(in: database, core: core, "SELECT * FROM $T WHERE \(sqlWhere)", arguments: arguments).map {
            let decoder = BlackbirdSQLiteDecoder(database: database, row: $0.row)
            let instance = try Self(from: decoder)
            if Self.enableCaching { instance._saveCachedInstance(for: database) }
            return instance
        }
    }

    /// Executes arbitrary SQL with a placeholder available for this type's table name.
    /// - Parameters:
    ///   - database: The ``Blackbird/Database`` instance to query.
    ///   - query: The SQL statement to execute, optionally containing:
    ///       - A `$T` placeholder which will be replaced with this type's table name.
    ///       - Question-mark placeholders (`?`) for any argument values to be passed to the query.
    ///   - arguments: Values corresponding to any placeholders in the query.
    /// - Returns: An array of rows matching the query if applicable, or an empty array otherwise.
    @discardableResult
    public static func query(in database: Blackbird.Database, _ query: String, _ arguments: Sendable...) async throws -> [Blackbird.ModelRow<Self>] {
        return try await self.query(in: database, query, arguments: arguments)
    }

    /// Executes arbitrary SQL with a placeholder available for this type's table name.
    /// - Parameters:
    ///   - database: The ``Blackbird/Database`` instance to query.
    ///   - query: The SQL statement to execute, optionally containing:
    ///       - A `$T` placeholder which will be replaced with this type's table name.
    ///       - Question-mark placeholders (`?`) for any argument values to be passed to the query.
    ///   - arguments: An array of values corresponding to any placeholders in the query.
    /// - Returns: An array of rows matching the query if applicable, or an empty array otherwise.
    @discardableResult
    public static func query(in database: Blackbird.Database, _ query: String, arguments: [Sendable]) async throws -> [Blackbird.ModelRow<Self>] {
        let arguments = try arguments.map { try Blackbird.Value.fromAny($0) }
        return try await _cacheableResult(database: database, tableName: Self.tableName, query: query, arguments: arguments) {
            try await _queryInternal(in: $0, query, arguments: arguments)
        }
    }

    internal static func _queryInternal(in database: Blackbird.Database, _ query: String, arguments: [Sendable]) async throws -> [Blackbird.ModelRow<Self>] {
        let table = Self.table
        try await table.resolveWithDatabase(type: Self.self, database: database, core: database.core) { try validateSchema(database: database) }
        return try await database.core.query(query.replacingOccurrences(of: "$T", with: table.name), arguments: arguments).map { Blackbird.ModelRow<Self>($0, table: table) }
    }

    /// Executes arbitrary SQL with a placeholder available for this type's table name.
    /// - Parameters:
    ///   - database: The ``Blackbird/Database`` instance to query.
    ///   - query: The SQL statement to execute, optionally containing:
    ///       - A `$T` placeholder which will be replaced with this type's table name.
    ///       - Named placeholders prefixed by a colon (`:`), at-sign (`@`), or dollar sign (`$`) as described in the [SQLite documentation](https://www.sqlite.org/c3ref/bind_blob.html).
    ///   - arguments: A dictionary of placeholder names used in the query and their corresponding values. Names must include the prefix character used.
    /// - Returns: An array of rows matching the query if applicable, or an empty array otherwise.
    @discardableResult
    public static func query(in database: Blackbird.Database, _ query: String, arguments: [String: Sendable]) async throws -> [Blackbird.ModelRow<Self>] {
        let table = Self.table
        try await table.resolveWithDatabase(type: Self.self, database: database, core: database.core) { try validateSchema(database: database) }
        return try await database.core.query(query.replacingOccurrences(of: "$T", with: table.name), arguments: arguments).map { Blackbird.ModelRow<Self>($0, table: table) }
    }

    /// Synchronous version of ``query(in:_:_:)`` for use when the database actor is isolated within calls to ``Blackbird/Database/transaction(_:)`` or ``Blackbird/Database/cancellableTransaction(_:)``.
    @discardableResult
    public static func queryIsolated(in database: Blackbird.Database, core: isolated Blackbird.Database.Core, _ query: String, _ arguments: Sendable...) throws -> [Blackbird.ModelRow<Self>] {
        return try self.queryIsolated(in: database, core: core, query, arguments: arguments)
    }

    /// Synchronous version of ``query(in:_:arguments:)-1bv0o`` for use when the database actor is isolated within calls to ``Blackbird/Database/transaction(_:)`` or ``Blackbird/Database/cancellableTransaction(_:)``.
    @discardableResult
    public static func queryIsolated(in database: Blackbird.Database, core: isolated Blackbird.Database.Core, _ query: String, arguments: [Sendable]) throws -> [Blackbird.ModelRow<Self>] {
        let arguments = try arguments.map { try Blackbird.Value.fromAny($0) }
        return try _cacheableResultIsolated(database: database, core: core, tableName: self.tableName, query: query, arguments: arguments) {
            try self._queryInternalIsolated(in: $0, core: $1, query, arguments: arguments)
        }
    }

    internal static func _queryInternalIsolated(in database: Blackbird.Database, core: isolated Blackbird.Database.Core, _ query: String, arguments: [Sendable]) throws -> [Blackbird.ModelRow<Self>] {
        let table = Self.table
        try table.resolveWithDatabaseIsolated(type: Self.self, database: database, core: core) { try Self.validateSchema(database: database) }
        return try core.query(query.replacingOccurrences(of: "$T", with: table.name), arguments: arguments).map { Blackbird.ModelRow<Self>($0, table: table) }
    }

    /// Synchronous version of ``query(in:_:arguments:)-3dwoy`` for use when the database actor is isolated within calls to ``Blackbird/Database/transaction(_:)`` or ``Blackbird/Database/cancellableTransaction(_:)``.
    @discardableResult
    public static func queryIsolated(in database: Blackbird.Database, core: isolated Blackbird.Database.Core, _ query: String, arguments: [String: Sendable]) throws -> [Blackbird.ModelRow<Self>] {
        let table = Self.table
        try table.resolveWithDatabaseIsolated(type: Self.self, database: database, core: core) { try validateSchema(database: database) }
        return try core.query(query.replacingOccurrences(of: "$T", with: table.name), arguments: arguments).map { Blackbird.ModelRow<Self>($0, table: table) }
    }

    internal static func validateSchema(database: Blackbird.Database) throws -> Void {
        var testRow = Blackbird.Row()
        for column in table.columns { testRow[column.name] = column.mayBeNull ? .null : column.type.defaultValue() }
        let decoder = BlackbirdSQLiteDecoder(database: database, row: testRow)
        do {
            _ = try Self(from: decoder)
        } catch {
            fatalError("Table \"\(tableName)\" definition defaults do not decode to model \(String(describing: self)): \(error)")
        }
    }

    /// Performs setup and any necessary schema migrations.
    ///
    /// Optional. If not called manually, setup and schema migrations will occur when the first database operation is performed for the conforming type's table.
    ///
    /// If the database migration fails, an error will be thrown.
    ///
    /// - Parameters:
    ///   - database: The ``Blackbird/Database`` instance to resolve the schema in.
    public static func resolveSchema(in database: Blackbird.Database) async throws {
        try await table.resolveWithDatabase(type: Self.self, database: database, core: database.core) { try validateSchema(database: database) }
    }
    
    /// The primary-key values of the current instance, as an array (to support multi-column primary keys).
    public func primaryKeyValues() throws -> [Any] {
        if Self.primaryKey.isEmpty {
            let mirror = Mirror(reflecting: self)
            for child in mirror.children {
                if child.label == "_id", let column = child.value as? any ColumnWrapper { return [column.value] }
            }
            fatalError("id value not found, and no other primary keys specified")
        }
        return Self.primaryKey.map { (self[keyPath: $0] as! any ColumnWrapper).value }
    }
    
    private func enumerateColumnValues(_ action: ((_ column: any ColumnWrapper, _ name: String, _ value: Blackbird.Value) -> Void)) throws {
        for (label, child) in Mirror(reflecting: self).children {
            guard var label, let column = child as? any ColumnWrapper else { continue }
            label = label.removingLeadingUnderscore()

            let value: Blackbird.Value
            if let optional = column.value as? OptionalProtocol {
                value = try Blackbird.Value.fromAny(optional.wrappedOptionalValue)
            } else {
                value = try Blackbird.Value.fromAny(column.value)
            }
            
            action(column, label, value)
        }
    }
    
    /// Write this instance to a database.
    /// - Parameter database: The ``Blackbird/Database`` instance to write to.
    public func write(to database: Blackbird.Database) async throws {
        try await writeIsolated(to: database, core: database.core)
    }
    
    /// Write this instance to a database synchronously from an actor-isolated transaction.
    /// - Parameters:
    ///   - database: The ``Blackbird/Database`` instance to write to.
    ///   - core: The isolated ``Blackbird/Database/Core`` provided to the transaction.
    ///
    /// For use only when the database actor is isolated within calls to ``Blackbird/Database/transaction(_:)`` or ``Blackbird/Database/cancellableTransaction(_:)``.
    public func writeIsolated(to database: Blackbird.Database, core: isolated Blackbird.Database.Core) throws {
        let table = Self.table
        if database.options.contains(.readOnly) { fatalError("Cannot write BlackbirdModel to a read-only database") }
        try table.resolveWithDatabaseIsolated(type: Self.self, database: database, core: core) { try Self.validateSchema(database: database) }

        var columnNames: [String] = []
        var placeholders: [String] = []
        var values: [Blackbird.Value] = []
        var valuesByColumnName: [String: Blackbird.Value] = [:]

        var changedColumnNames = Blackbird.ColumnNames()
        var changedColumns: [any ColumnWrapper] = []

        try enumerateColumnValues { column, name, value in
            values.append(value)
            valuesByColumnName[name] = value
            columnNames.append(name)
            placeholders.append("?")

            if column.hasChanged(in: database) {
                changedColumnNames.insert(name)
                changedColumns.append(column)
            }
        }
        
        if changedColumns.isEmpty { return }

        let primaryKeyValues = table.primaryKeys.map { valuesByColumnName[$0.name]! }
      
        // Intentionally using "INSERT INTO ... ON CONFLICT (primary key) DO UPDATE..."
        //  instead of "REPLACE INTO". This way, primary-key duplicates are treated as UPDATEs,
        //  but conflicts in UNIQUE indexes fail and throw Error.uniqueConstraintFailed.
        //
        let sql = "INSERT INTO `\(table.name)` (`\(columnNames.joined(separator: "`,`"))`) VALUES (\(placeholders.joined(separator: ","))) \(table.upsertClause)"

        database.changeReporter.ignoreWritesToTable(Self.tableName)
        defer {
            database.changeReporter.stopIgnoringWrites()
            database.changeReporter.reportChange(tableName: Self.tableName, primaryKey: primaryKeyValues, changedColumns: changedColumnNames)
            if Self.enableCaching { self._saveCachedInstance(for: database) }
        }
        try core.query(sql, arguments: values)
        for column in changedColumns { column.clearHasChanged(in: database) }
    }

    /// Delete this instance from a database.
    /// - Parameter database: The ``Blackbird/Database`` instance to delete from.
    public func delete(from database: Blackbird.Database) async throws {
        try await deleteIsolated(from: database, core: database.core)
    }

    /// Delete this instance from a database synchronously from an actor-isolated transaction.
    /// - Parameters:
    ///   - database: The ``Blackbird/Database`` instance to delete from.
    ///   - core: The isolated ``Blackbird/Database/Core`` provided to the transaction.
    ///
    /// For use only when the database actor is isolated within calls to ``Blackbird/Database/transaction(_:)`` or ``Blackbird/Database/cancellableTransaction(_:)``.
    public func deleteIsolated(from database: Blackbird.Database, core: isolated Blackbird.Database.Core) throws {
        if database.options.contains(.readOnly) { fatalError("Cannot delete BlackbirdModel from a read-only database") }
        let table = Self.table
        try table.resolveWithDatabaseIsolated(type: Self.self, database: database, core: core) { try Self.validateSchema(database: database) }

        let values = try self.primaryKeyValues().map { try Blackbird.Value.fromAny($0) }
        let andClauses: [String] = table.primaryKeys.map { "`\($0.name)` = ?" }
        
        database.changeReporter.ignoreWritesToTable(Self.tableName)
        defer {
            database.changeReporter.stopIgnoringWrites()
            database.changeReporter.reportChange(tableName: Self.tableName, primaryKey: values, changedColumns: table.columnNames)
        }
        let sql = "DELETE FROM `\(Self.tableName)` WHERE \(andClauses.joined(separator: " AND "))"
        try core.query(sql, arguments: values)
        if Self.enableCaching { self._deleteCachedInstance(for: database) }
    }

    fileprivate static func _cacheableResult<T>(database: Blackbird.Database, tableName: String, query: String, arguments: [Blackbird.Value], resultFetcher: ((Blackbird.Database) async throws -> T)) async throws -> T {
        guard Self.enableCaching else { return try await resultFetcher(database) }
        var cacheKey: [Blackbird.Value] = [.text(query)]
        cacheKey.append(contentsOf: arguments)
        if let cachedResult = database.cache.readQueryResult(tableName: tableName, cacheKey: cacheKey) as? T { return cachedResult }
        
        let result = try await resultFetcher(database)
        database.cache.writeQueryResult(tableName: tableName, cacheKey: cacheKey, result: result)
        return result
    }

    fileprivate static func _cacheableResultIsolated<T>(database: Blackbird.Database, core: isolated Blackbird.Database.Core, tableName: String, query: String, arguments: [Blackbird.Value], resultFetcher: ((Blackbird.Database, isolated Blackbird.Database.Core) throws -> T)) throws -> T {
        guard Self.enableCaching else { return try resultFetcher(database, core) }
        var cacheKey: [Blackbird.Value] = [.text(query)]
        cacheKey.append(contentsOf: arguments)
        if let cachedResult = database.cache.readQueryResult(tableName: tableName, cacheKey: cacheKey) as? T { return cachedResult }
        
        let result = try resultFetcher(database, core)
        database.cache.writeQueryResult(tableName: tableName, cacheKey: cacheKey, result: result)
        return result
    }
}
