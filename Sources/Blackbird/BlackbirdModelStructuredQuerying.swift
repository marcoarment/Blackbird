//
//  BlackbirdModelStructuredQuerying.swift
//  Created by Marco Arment on 3/11/23.
//  Copyright (c) 2023 Marco Arment
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

extension PartialKeyPath: @unchecked Sendable { }

public extension String.StringInterpolation {
    mutating func appendInterpolation<T: BlackbirdModel>(_ keyPath: T.BlackbirdColumnKeyPath) {
        let table = SchemaGenerator.shared.table(for: T.self)
        appendLiteral(table.keyPathToColumnName(keyPath: keyPath))
    }
}

/// A column key-paths and direction to be used in SQL queries as `ORDER BY` clauses.
///
/// Specify as either:
/// - `.ascending(keyPath)`: equivalent to `ORDER BY keyPath` in SQL
/// - `.descending(keyPath)`: equivalent to `ORDER BY keyPath DESC` in SQL
///
/// Used as a `orderBy:` expression in ``BlackbirdModel`` functions such as:
/// - ``BlackbirdModel/query(in:columns:matching:orderBy:limit:)``
/// - ``BlackbirdModel/read(from:matching:orderBy:limit:)``
public struct BlackbirdModelOrderClause<T: BlackbirdModel>: Sendable {
    public enum Direction: Sendable {
        case ascending
        case descending
    }
    
    let column: T.BlackbirdColumnKeyPath
    let direction: Direction
    
    public static func ascending(_ column: T.BlackbirdColumnKeyPath) -> BlackbirdModelOrderClause { BlackbirdModelOrderClause(column, direction: .ascending) }
    public static func descending(_ column: T.BlackbirdColumnKeyPath) -> BlackbirdModelOrderClause { BlackbirdModelOrderClause(column, direction: .descending) }
    
    init(_ column: T.BlackbirdColumnKeyPath, direction: Direction) {
        self.column = column
        self.direction = direction
    }
    
    func orderByClause(table: Blackbird.Table) -> String {
        let columnName = table.keyPathToColumnName(keyPath: column)
        return "`\(columnName)`\(direction == .descending ? " DESC" : "")"
    }
}
    
extension BlackbirdModel {
    fileprivate static func _decodeStructuredQuery(operation: String = "SELECT * FROM", selectColumnSubset: [BlackbirdColumnKeyPath]? = nil,  matching: BlackbirdModelColumnExpression<Self>? = nil, updating: [BlackbirdColumnKeyPath : Sendable] = [:], orderBy: [BlackbirdModelOrderClause<Self>] = [], limit: Int? = nil) -> (query: String, arguments: [Sendable], changedColumns: Blackbird.ColumnNames) {
        let table = SchemaGenerator.shared.table(for: Self.self)
        var clauses: [String] = []
        var arguments: [Sendable] = []
        var operation = operation
        var matching = matching
        
        if let selectColumnSubset {
            let columnList = selectColumnSubset.map { table.keyPathToColumnName(keyPath: $0) }.joined(separator: "`,`")
            operation = "SELECT `\(columnList)` FROM"
        }

        var setClauses: [String] = []
        var changedColumns = Blackbird.ColumnNames()
        var updateWhereNotMatchingExpr: BlackbirdModelColumnExpression<Self>? = nil
        for (keyPath, value) in updating {
            let columnName = table.keyPathToColumnName(keyPath: keyPath)
            changedColumns.insert(columnName)
            setClauses.append("`\(columnName)` = ?")
            arguments.append(value)
            
            // In an UPDATE query, SQLite will call the update hook and report a change on EVERY row
            // that matches the WHERE clause (or every row in the table without a WHERE) and report it
            // as changed, even if no rows matched and therefore no data was changed. E.g.:
            //
            //   UPDATE t SET a = NULL, b = 2; -- in a table with X rows, this reports X rows changed
            //   UPDATE t SET a = NULL, b = 2; -- ALSO reports X rows changed, even though none actually were
            //
            // So we add automatic WHERE clauses corresponding to each SET value to make it like this:
            //
            //   UPDATE t SET a = NULL, b = 2 WHERE a IS NOT NULL OR b != 2;
            //
            // ...which makes SQLite properly report only the actually-changed rows.
            //
            if updateWhereNotMatchingExpr != nil {
                updateWhereNotMatchingExpr = updateWhereNotMatchingExpr! || keyPath != value
            } else {
                updateWhereNotMatchingExpr = keyPath != value
            }
        }
        if !setClauses.isEmpty {
            clauses.append("SET \(setClauses.joined(separator: ","))")

            if matching != nil, let updateWhereNotMatchingExpr {
                matching = matching! && updateWhereNotMatchingExpr
            } else {
                matching = updateWhereNotMatchingExpr
            }
        }

        if let matching {
            let (whereClause, whereArguments) = matching.compile(table: table)
            if let whereClause { clauses.append("WHERE \(whereClause)") }
            arguments.append(contentsOf: whereArguments)
        }

        if !orderBy.isEmpty {
            let orderByClause = orderBy.map { $0.orderByClause(table: table) }.joined(separator: ",")
            clauses.append("ORDER BY \(orderByClause)")
        }
        
        if let limit { clauses.append("LIMIT \(limit)") }
        
        let query = "\(operation) \(table.name)\(clauses.isEmpty ? "" : " \(clauses.joined(separator: " "))")"

        return (query: query, arguments: arguments, changedColumns: changedColumns)
    }

    /// Get the number of rows in this BlackbirdModel's table.
    ///
    /// - Parameters:
    ///   - database: The ``Blackbird/Database`` instance to read from.
    ///   - matching: An optional filtering expression using column key-paths, e.g. `\.$id > 100`, to be used in the resulting SQL query as a `WHERE` clause. See ``BlackbirdModelColumnExpression``.
    ///
    ///     If not specified, all rows in the table will be counted.
    /// - Returns: The number of matching rows.
    ///
    /// ## Example
    /// ```swift
    /// let c = try await Post.count(in: db, matching: \.$id > 100)
    /// // Equivalent to:
    /// // "SELECT COUNT(*) FROM Post WHERE id > 100"
    /// ```
    public static func count(in database: Blackbird.Database, matching: BlackbirdModelColumnExpression<Self>? = nil) async throws -> Int {
        let decoded = _decodeStructuredQuery(operation: "SELECT COUNT(*) FROM", matching: matching)
        return try await query(in: database, decoded.query, arguments: decoded.arguments).first!["COUNT(*)"]!.intValue!
    }

    /// Synchronous version of ``count(in:matching:)``  for use when the database actor is isolated within calls to ``Blackbird/Database/transaction(_:)`` or ``Blackbird/Database/cancellableTransaction(_:)``.
    public static func countIsolated(in database: Blackbird.Database, core: isolated Blackbird.Database.Core, matching: BlackbirdModelColumnExpression<Self>? = nil) async throws -> Int {
        let decoded = _decodeStructuredQuery(operation: "SELECT COUNT(*) FROM", matching: matching)
        return try queryIsolated(in: database, core: core, decoded.query, arguments: decoded.arguments).first!["COUNT(*)"]!.intValue!
    }


    /// Reads instances from a database using key-path equality tests.
    ///
    /// - Parameters:
    ///   - database: The ``Blackbird/Database`` instance to read from.
    ///   - matching: An optional filtering expression using column key-paths, e.g. `\.$id == 1`, to be used in the resulting SQL query as a `WHERE` clause. See ``BlackbirdModelColumnExpression``.
    ///   - orderBy: An optional series of column key-paths to order the results by, represented as:
    ///     - `.ascending(keyPath)`: equivalent to SQL `ORDER BY keyPath`
    ///     - `.descending(keyPath)`: equivalent to SQL `ORDER BY keyPath DESC`
    ///
    ///     If not specified, the order of results is undefined.
    ///   - limit: An optional limit to how many results will be returned. If not specified, all matching results will be returned.
    /// - Returns: An array of decoded instances matching the query.
    ///
    /// ## Example
    /// ```swift
    /// let posts = try await Post.read(
    ///     from: db,
    ///     matching: \.$id == 123 && \.$title == "Hi",
    ///     orderBy: .ascending(\.$id),
    ///     limit: 1
    /// )
    /// // Equivalent to:
    /// // "SELECT * FROM Post WHERE id = 123 AND title = 'Hi' ORDER BY id LIMIT 1"
    /// ```
    public static func read(from database: Blackbird.Database, matching: BlackbirdModelColumnExpression<Self>? = nil, orderBy: BlackbirdModelOrderClause<Self> ..., limit: Int? = nil) async throws -> [Self] {
        let decoded = _decodeStructuredQuery(matching: matching, orderBy: orderBy, limit: limit)
        return try await query(in: database, decoded.query, arguments: decoded.arguments).map {
            let decoder = BlackbirdSQLiteDecoder(database: database, row: $0.row)
            return try Self(from: decoder)
        }
    }

    /// Synchronous version of ``read(from:matching:orderBy:limit:)``  for use when the database actor is isolated within calls to ``Blackbird/Database/transaction(_:)`` or ``Blackbird/Database/cancellableTransaction(_:)``.
    public static func readIsolated(from database: Blackbird.Database, core: isolated Blackbird.Database.Core, matching: BlackbirdModelColumnExpression<Self>? = nil, orderBy: BlackbirdModelOrderClause<Self> ..., limit: Int? = nil) throws -> [Self] {
        let decoded = _decodeStructuredQuery(matching: matching, orderBy: orderBy, limit: limit)
        return try queryIsolated(in: database, core: core, decoded.query, arguments: decoded.arguments).map {
            let decoder = BlackbirdSQLiteDecoder(database: database, row: $0.row)
            return try Self(from: decoder)
        }
    }

    /// Selects a subset of the table's columns matching the given column values, using column key-paths for this model type.
    /// - Parameters:
    ///   - database: The ``Blackbird/Database`` instance to query.
    ///   - selectColumns: An array of column key-paths of this BlackbirdModel type. The returned rows will contain only these columns.
    ///   - matching: An optional filtering expression using column key-paths, e.g. `\.$id == 1`, to be used in the resulting SQL query as a `WHERE` clause. See ``BlackbirdModelColumnExpression``.
    ///   - orderBy: An optional series of column key-paths to order the results by, represented as:
    ///     - `.ascending(keyPath)`: equivalent to SQL `ORDER BY keyPath`
    ///     - `.descending(keyPath)`: equivalent to SQL `ORDER BY keyPath DESC`
    ///
    ///     If not specified, the order of results is undefined.
    ///   - limit: An optional limit to how many results will be returned. If not specified, all matching results will be returned.
    /// - Returns: An array of matching rows, each containing only the columns specified.
    public static func query(in database: Blackbird.Database, columns: [BlackbirdColumnKeyPath], matching: BlackbirdModelColumnExpression<Self>? = nil, orderBy: BlackbirdModelOrderClause<Self> ..., limit: Int? = nil) async throws -> [Blackbird.ModelRow<Self>] {
        let decoded = _decodeStructuredQuery(selectColumnSubset: columns, matching: matching, orderBy: orderBy, limit: limit)
        return try await query(in: database, decoded.query, arguments: decoded.arguments)
    }

    /// Synchronous version of ``query(in:columns:matching:orderBy:limit:)`` for use when the database actor is isolated within calls to ``Blackbird/Database/transaction(_:)`` or ``Blackbird/Database/cancellableTransaction(_:)``.
    public static func queryIsolated(in database: Blackbird.Database, core: isolated Blackbird.Database.Core, columns: [BlackbirdColumnKeyPath], matching: BlackbirdModelColumnExpression<Self>? = nil, orderBy: BlackbirdModelOrderClause<Self> ..., limit: Int? = nil) throws -> [Blackbird.ModelRow<Self>] {
        let decoded = _decodeStructuredQuery(selectColumnSubset: columns, matching: matching, orderBy: orderBy, limit: limit)
        return try queryIsolated(in: database, core: core, decoded.query, arguments: decoded.arguments)
    }

    /// Changes a subset of the table's rows matching the given column values, using column key-paths for this model type.
    /// - Parameters:
    ///   - database: The ``Blackbird/Database`` instance to query.
    ///   - changes: A dictionary of column key-paths of this BlackbirdModel type and corresponding values to set them to, e.g. `[ \.$title : "New title" ]`.
    ///   - matching: A filtering expression using column key-paths, e.g. `\.$id == 1`, to be used in the resulting SQL query as a `WHERE` clause. See ``BlackbirdModelColumnExpression``.
    ///
    ///       Use `.all` to delete all rows in the table (executes an SQL `UPDATE` without a `WHERE` clause).
    /// - Returns: An array of matching rows, each containing only the columns specified.
    ///
    /// ## Example
    /// ```swift
    /// try await Post.update(
    ///     in: db,
    ///     set: [ \.$title = "Hi" ]
    ///     matching: \.$id == 123
    /// )
    /// // Equivalent to:
    /// // "UPDATE Post SET title = 'Hi' WHERE id = 123"
    /// ```
    public static func update(in database: Blackbird.Database, set changes: [BlackbirdColumnKeyPath : Sendable], matching: BlackbirdModelColumnExpression<Self>) async throws {
        if changes.isEmpty { return }
        try await updateIsolated(in: database, core: database.core, set: changes, matching: matching)
    }

    /// Synchronous version of ``update(in:set:matching:)`` for use when the database actor is isolated within calls to ``Blackbird/Database/transaction(_:)`` or ``Blackbird/Database/cancellableTransaction(_:)``.
    public static func updateIsolated(in database: Blackbird.Database, core: isolated Blackbird.Database.Core, set changes: [BlackbirdColumnKeyPath : Sendable], matching: BlackbirdModelColumnExpression<Self>) throws {
        if database.options.contains(.readOnly) { fatalError("Cannot update BlackbirdModels in a read-only database") }
        if changes.isEmpty { return }
        let table = Self.table
        try table.resolveWithDatabaseIsolated(type: Self.self, database: database, core: core) { try Self.validateSchema(database: database) }
        let decoded = _decodeStructuredQuery(operation: "UPDATE", matching: matching, updating: changes)

        let changeCountBefore = core.changeCount
        database.changeReporter.ignoreWritesToTable(Self.tableName)
        defer {
            database.changeReporter.stopIgnoringWrites()
            if core.changeCount != changeCountBefore {
                database.changeReporter.reportChange(tableName: Self.tableName, primaryKey: nil, changedColumns: decoded.changedColumns)
            }
        }
        try core.query(decoded.query, arguments: decoded.arguments)
    }

    /// Deletes a subset of the table's columns matching the given column values, using column key-paths for this model type.
    /// - Parameters:
    ///   - database: The ``Blackbird/Database`` instance to query.
    ///   - matching: A filtering expression using column key-paths, e.g. `\.$id == 1`, to be used in the resulting SQL query as a `WHERE` clause. See ``BlackbirdModelColumnExpression``.
    ///
    ///       Use `.all` to delete all rows in the table (executes an SQL `DELETE` without a `WHERE` clause).
    /// - Returns: An array of matching rows, each containing only the columns specified.
    ///
    /// ## Example
    /// ```swift
    /// try await Post.delete(in: db, matching: \.$id == 123)
    /// // Equivalent to:
    /// // "DELETE FROM Post WHERE id = 123"
    /// ```
    public static func delete(from database: Blackbird.Database, matching: BlackbirdModelColumnExpression<Self>) async throws {
        try await deleteIsolated(from: database, core: database.core, matching: matching)
    }

    /// Synchronous version of ``delete(from:matching:)`` for use when the database actor is isolated within calls to ``Blackbird/Database/transaction(_:)`` or ``Blackbird/Database/cancellableTransaction(_:)``.
    public static func deleteIsolated(from database: Blackbird.Database, core: isolated Blackbird.Database.Core, matching: BlackbirdModelColumnExpression<Self>) throws {
        if database.options.contains(.readOnly) { fatalError("Cannot delete BlackbirdModels from a read-only database") }
        let table = Self.table
        try table.resolveWithDatabaseIsolated(type: Self.self, database: database, core: core) { try Self.validateSchema(database: database) }

        let decoded = _decodeStructuredQuery(operation: "DELETE FROM", matching: matching)
        try queryIsolated(in: database, core: core, decoded.query, arguments: decoded.arguments)
    }
}

//MARK: - Where-expression DSL

/*
    This is what enables the "matching:" parameters with structured properties like this:
    
        Test.read(from: db, matching: \.$id == 123)
        Test.read(from: db, matching: \.$id == 123 && \.$title == "Hi" || \.$id > 2)
        Test.read(from: db, matching: \.$url != nil)
    
    ...by overriding those operators on BlackbirdColumnKeyPaths to return BlackbirdModelColumnExpressions.

 */

public func == <T: BlackbirdModel> (lhs: T.BlackbirdColumnKeyPath, rhs: Sendable?) -> BlackbirdModelColumnExpression<T> {
    if let rhs { return .equals(lhs, rhs) } else { return .isNull(lhs) }
}

public func != <T: BlackbirdModel> (lhs: T.BlackbirdColumnKeyPath, rhs: Sendable?) -> BlackbirdModelColumnExpression<T> {
    if let rhs { return .notEquals(lhs, rhs) } else { return .isNotNull(lhs) }
}

public func <  <T: BlackbirdModel> (lhs: T.BlackbirdColumnKeyPath, rhs: Sendable) -> BlackbirdModelColumnExpression<T> { .lessThan(lhs, rhs) }
public func >  <T: BlackbirdModel> (lhs: T.BlackbirdColumnKeyPath, rhs: Sendable) -> BlackbirdModelColumnExpression<T> { .greaterThan(lhs, rhs) }
public func <=  <T: BlackbirdModel> (lhs: T.BlackbirdColumnKeyPath, rhs: Sendable) -> BlackbirdModelColumnExpression<T> { .lessThanOrEqual(lhs, rhs) }
public func >=  <T: BlackbirdModel> (lhs: T.BlackbirdColumnKeyPath, rhs: Sendable) -> BlackbirdModelColumnExpression<T> { .greaterThanOrEqual(lhs, rhs) }
public func && <T: BlackbirdModel> (lhs: BlackbirdModelColumnExpression<T>, rhs: BlackbirdModelColumnExpression<T>) -> BlackbirdModelColumnExpression<T> { .and(lhs, rhs) }
public func || <T: BlackbirdModel> (lhs: BlackbirdModelColumnExpression<T>, rhs: BlackbirdModelColumnExpression<T>) -> BlackbirdModelColumnExpression<T> { .or(lhs, rhs) }


/// A filtering expression using column key-paths to be used in SQL queries as `WHERE` clauses.
///
/// Supported operators:
/// - `==`, `!=`, `<`, `>`, `<=`, and `>=`, where the left-hand operand is a column key-path and the right-hand operand is a SQL-compatible value or `nil`.
/// - `||` or `&&` to combine multiple expressions.
///
/// Examples:
/// - `.all`: equivalent to not using a `WHERE` clause
/// - `\.$id == 1`: equivalent to `WHERE id = 1`
/// - `\.$id > 1`: equivalent to `WHERE id > 1`
/// - `\.$id >= 1`: equivalent to `WHERE id >= 1`
/// - `\.$id < 1`: equivalent to `WHERE id < 1`
/// - `\.$id <= 1`: equivalent to `WHERE id <= 1`
/// - `\.$id == nil`: equivalent to `WHERE id IS NULL`
/// - `\.$id != nil`: equivalent to `WHERE id IS NOT NULL`
/// - `\.$id > 0 && \.$title != "a"`: equivalent to `WHERE id > 0 AND title != 'a'`
/// - `\.$id != nil || \.$title == nil`: equivalent to `WHERE id IS NOT NULL OR title IS NULL`
/// - `.literal("id % 3 = ?", 1)`: equivalent to `WHERE id % 3 = 1`
///
/// Used as a `matching:` expression in ``BlackbirdModel`` functions such as:
/// - ``BlackbirdModel/query(in:columns:matching:orderBy:limit:)``
/// - ``BlackbirdModel/read(from:matching:orderBy:limit:)``
/// - ``BlackbirdModel/update(in:set:matching:)``
/// - ``BlackbirdModel/delete(from:matching:)``
public struct BlackbirdModelColumnExpression<T: BlackbirdModel>: Sendable, BlackbirdQueryExpression {

    /// Use `.all` to operate on all rows in the table without a `WHERE` clause.
    public static var all: Self {
        get {
            BlackbirdModelColumnExpression<T>()
        }
    }

    internal enum BinaryOperator: String, Sendable {
        case equal = "="
        case notEqual = "!="
        case lessThan = "<"
        case greaterThan = ">"
        case lessThanOrEqual = "<="
        case greaterThanOrEqual = ">="
    }

    internal enum UnaryOperator: String, Sendable {
        case isNull = "IS NULL"
        case isNotNull = "IS NOT NULL"
    }

    internal enum CombiningOperator: String, Sendable {
        case and = "AND"
        case or = "OR"
    }
    
    private let expression: BlackbirdQueryExpression

    init(column: T.BlackbirdColumnKeyPath, sqlOperator: UnaryOperator) {
        expression = BlackbirdColumnUnaryExpression(column: column, sqlOperator: sqlOperator)
    }

    init(column: T.BlackbirdColumnKeyPath, sqlOperator: BinaryOperator, value: Sendable) {
        expression = BlackbirdColumnBinaryExpression(column: column, sqlOperator: sqlOperator, value: value)
    }

    init(lhs: BlackbirdModelColumnExpression<T>, sqlOperator: CombiningOperator, rhs: BlackbirdModelColumnExpression<T>) {
        expression = BlackbirdCombiningExpression(lhs: lhs, rhs: rhs, sqlOperator: sqlOperator)
    }

    init(expressionLiteral: String, arguments: [Sendable]) {
        expression = BlackbirdColumnLiteralExpression(literal: expressionLiteral, arguments: arguments)
    }

    init() {
        expression = BlackbirdColumnNoExpression()
    }

    internal func compile(table: Blackbird.Table) -> (whereClause: String?, values: [Sendable]) { expression.compile(table: table) }

    static func isNull<T: BlackbirdModel>(_ columnKeyPath: T.BlackbirdColumnKeyPath) -> BlackbirdModelColumnExpression<T> {
        BlackbirdModelColumnExpression<T>(column: columnKeyPath, sqlOperator: .isNull)
    }

    static func isNotNull<T: BlackbirdModel>(_ columnKeyPath: T.BlackbirdColumnKeyPath) -> BlackbirdModelColumnExpression<T> {
        BlackbirdModelColumnExpression<T>(column: columnKeyPath, sqlOperator: .isNotNull)
    }

    static func equals<T: BlackbirdModel>(_ columnKeyPath: T.BlackbirdColumnKeyPath, _ value: Sendable) -> BlackbirdModelColumnExpression<T> {
        BlackbirdModelColumnExpression<T>(column: columnKeyPath, sqlOperator: .equal, value: value)
    }

    static func notEquals<T: BlackbirdModel>(_ columnKeyPath: T.BlackbirdColumnKeyPath, _ value: Sendable) -> BlackbirdModelColumnExpression<T> {
        BlackbirdModelColumnExpression<T>(column: columnKeyPath, sqlOperator: .notEqual, value: value)
    }

    static func lessThan<T: BlackbirdModel>(_ columnKeyPath: T.BlackbirdColumnKeyPath, _ value: Sendable) -> BlackbirdModelColumnExpression<T> {
        BlackbirdModelColumnExpression<T>(column: columnKeyPath, sqlOperator: .lessThan, value: value)
    }

    static func greaterThan<T: BlackbirdModel>(_ columnKeyPath: T.BlackbirdColumnKeyPath, _ value: Sendable) -> BlackbirdModelColumnExpression<T> {
        BlackbirdModelColumnExpression<T>(column: columnKeyPath, sqlOperator: .greaterThan, value: value)
    }

    static func lessThanOrEqual<T: BlackbirdModel>(_ columnKeyPath: T.BlackbirdColumnKeyPath, _ value: Sendable) -> BlackbirdModelColumnExpression<T> {
        BlackbirdModelColumnExpression<T>(column: columnKeyPath, sqlOperator: .lessThanOrEqual, value: value)
    }

    static func greaterThanOrEqual<T: BlackbirdModel>(_ columnKeyPath: T.BlackbirdColumnKeyPath, _ value: Sendable) -> BlackbirdModelColumnExpression<T> {
        BlackbirdModelColumnExpression<T>(column: columnKeyPath, sqlOperator: .greaterThanOrEqual, value: value)
    }

    static func and<T: BlackbirdModel>(_ lhs: BlackbirdModelColumnExpression<T>, _ rhs: BlackbirdModelColumnExpression<T>) -> BlackbirdModelColumnExpression<T> {
        BlackbirdModelColumnExpression<T>(lhs: lhs, sqlOperator: .and, rhs: rhs)
    }

    static func or<T: BlackbirdModel>(_ lhs: BlackbirdModelColumnExpression<T>, _ rhs: BlackbirdModelColumnExpression<T>) -> BlackbirdModelColumnExpression<T> {
        BlackbirdModelColumnExpression<T>(lhs: lhs, sqlOperator: .or, rhs: rhs)
    }

    /// Specify a literal expression to be used in a `WHERE` clause.
    ///
    /// Example: `.literal("id % 5 = ?", 1)`
    public static func literal<T: BlackbirdModel>(_ expressionLiteral: String, _ arguments: Sendable...) -> BlackbirdModelColumnExpression<T> {
        BlackbirdModelColumnExpression<T>(expressionLiteral: expressionLiteral, arguments: arguments)
    }
}


internal protocol BlackbirdQueryExpression: Sendable {
    func compile(table: Blackbird.Table) -> (whereClause: String?, values: [Sendable])
}

internal struct BlackbirdColumnNoExpression: BlackbirdQueryExpression {
    func compile(table: Blackbird.Table) -> (whereClause: String?, values: [Sendable]) {
        return (whereClause: nil, values: [])
    }
}

internal struct BlackbirdColumnBinaryExpression<T: BlackbirdModel>: BlackbirdQueryExpression {
    let column: T.BlackbirdColumnKeyPath
    let sqlOperator: BlackbirdModelColumnExpression<T>.BinaryOperator
    let value: Sendable

    func compile(table: Blackbird.Table) -> (whereClause: String?, values: [Sendable]) {
        var whereClause = "`\(table.keyPathToColumnName(keyPath: column))` \(sqlOperator.rawValue) ?"
        let value = try! Blackbird.Value.fromAny(value)
        var values = [value]
        if value == .null {
            if sqlOperator == .equal         { values = [] ; whereClause = "`\(table.keyPathToColumnName(keyPath: column))` IS NULL" }
            else if sqlOperator == .notEqual { values = [] ; whereClause = "`\(table.keyPathToColumnName(keyPath: column))` IS NOT NULL" }
        }
        return (whereClause: whereClause, values: values)
    }
}

internal struct BlackbirdColumnLiteralExpression: BlackbirdQueryExpression {
    let literal: String
    let arguments: [Sendable]
    
    func compile(table: Blackbird.Table) -> (whereClause: String?, values: [Sendable]) {
        return (whereClause: "\(literal)", values: arguments)
    }
}

internal struct BlackbirdColumnUnaryExpression<T: BlackbirdModel>: BlackbirdQueryExpression {
    let column: T.BlackbirdColumnKeyPath
    let sqlOperator: BlackbirdModelColumnExpression<T>.UnaryOperator

    func compile(table: Blackbird.Table) -> (whereClause: String?, values: [Sendable]) {
        return (whereClause: "`\(table.keyPathToColumnName(keyPath: column))` \(sqlOperator.rawValue)", values: [])
    }
}

internal struct BlackbirdCombiningExpression<T: BlackbirdModel>: BlackbirdQueryExpression {
    let lhs: BlackbirdQueryExpression
    let rhs: BlackbirdQueryExpression
    let sqlOperator: BlackbirdModelColumnExpression<T>.CombiningOperator

    func compile(table: Blackbird.Table) -> (whereClause: String?, values: [Sendable]) {
        let l = lhs.compile(table: table)
        let r = rhs.compile(table: table)
        
        var combinedValues = l.values
        combinedValues.append(contentsOf: r.values)
        
        var wheres: [String] = []
        if let whereL = l.whereClause { wheres.append(whereL) }
        if let whereR = r.whereClause { wheres.append(whereR) }
        return (whereClause: "(\(wheres.joined(separator: " \(sqlOperator.rawValue) ")))", values: combinedValues)
    }
}
