//
//  BlackbirdSchema.swift
//  Created by Marco Arment on 11/18/22.
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

// MARK: - Schema

extension Blackbird {

    /// The SQLite data type for a ``Column`` in a ``Table``.
    ///
    /// See  [SQLite data types](https://www.sqlite.org/datatype3.html) for implementation and storage details.
    ///
    /// These default values are used for non-`NULL` columns:
    ///
    /// Type | Default Value
    /// ---------|-------
    /// `.integer` | `0`
    /// `.double` | `0.0`
    /// `.text` |  Empty string (`""`)
    /// `.data` |  Empty data
    ///
    /// Custom default values for columns are not supported by Blackbird.
    public enum ColumnType {

        /// Stored as a **signed** integer up to 64-bit.
        ///
        /// **Default value:** `0`
        case integer
        
        /// Stored as a 64-bit floating-point number.
        ///
        /// **Default value:** `0.0`
        case double
        
        /// Stored as a UTF-8 string.
        ///
        /// **Default value:** empty string (`""`)
        case text
        
        /// Stored as an unmodified binary blob.
        ///
        /// **Default value:** empty data
        case data
        
        internal static func parseType(_ str: String) -> ColumnType? {
            if str.hasPrefix("TEXT") { return .text }
            if str.hasPrefix("INT") || str.hasPrefix("BOOL") { return .integer }
            if str.hasPrefix("FLOAT") || str.hasPrefix("DOUBLE") || str.hasPrefix("REAL") || str.hasPrefix("NUMERIC") { return .double }
            if str.hasPrefix("BLOB") { return .data }
            return nil
        }
        
        internal func definition() -> String {
            switch self {
                case .integer: return "INTEGER"
                case .double:  return "DOUBLE"
                case .text:    return "TEXT"
                case .data:    return "BLOB"
            }
        }
        
        internal func defaultValue() -> Value {
            switch self {
                case .integer: return .integer(0)
                case .double:  return .double(0)
                case .text:    return .text("")
                case .data:    return .data(Data())
            }
        }
    }

    /// A column in a ``Table``.
    public struct Column: Equatable, Hashable {
        enum Error: Swift.Error {
            case cannotParseColumnDefinition(table: String, description: String)
        }
    
        // intentionally ignoring primaryKeyIndex since it's only used for internal sorting
        public static func == (lhs: Self, rhs: Self) -> Bool { lhs.name == rhs.name && lhs.type == rhs.type && lhs.mayBeNull == rhs.mayBeNull }
        public func hash(into hasher: inout Hasher) {
            hasher.combine(name)
            hasher.combine(type)
            hasher.combine(mayBeNull)
        }
    
        internal let name: String
        internal let type: ColumnType
        internal let mayBeNull: Bool
        
        internal let primaryKeyIndex: Int // Only used for sorting, not considered for equality
        
        internal func definition() -> String {
            "`\(name)` \(type.definition()) \(mayBeNull ? "NULL" : "NOT NULL") DEFAULT \((mayBeNull ? .null : type.defaultValue()).sqliteLiteral())"
        }
        
        
        /// Defines a single column in a ``Blackbird/Table``.
        /// - Parameters:
        ///   - name: The column name. Must not be empty.
        ///   - type: A ``Blackbird/ColumnType``.
        ///   - mayBeNull: Whether this column may be `NULL` in the database and `nil` in the model, which requires the corresponding model property to be an optional type.
        public init(name: String, type: ColumnType, mayBeNull: Bool = false) {
            self.name = name
            self.type = type
            self.mayBeNull = mayBeNull
            self.primaryKeyIndex = 0
        }
        
        internal init(row: Row, tableName: String) throws {
            guard
                let name = row["name"]?.stringValue,
                let typeStr = row["type"]?.stringValue,
                let notNull = row["notnull"]?.boolValue,
                let primaryKeyIndex = row["pk"]?.intValue
            else { throw Error.cannotParseColumnDefinition(table: tableName, description: "Unexpected format from PRAGMA table_info") }
            
            guard let type = ColumnType.parseType(typeStr) else { throw Error.cannotParseColumnDefinition(table: tableName, description: "Column \"\(name)\" has unsupported type: \"\(typeStr)\"") }
            self.name = name
            self.type = type
            self.mayBeNull = !notNull
            self.primaryKeyIndex = primaryKeyIndex
        }
    }
    
    /// An index in a ``Table``.
    public struct Index: Equatable, Hashable {
        public enum Error: Swift.Error {
            case cannotParseIndexDefinition(definition: String, description: String)
        }
    
        public static func == (lhs: Self, rhs: Self) -> Bool { return lhs.name == rhs.name && lhs.unique == rhs.unique && lhs.columnNames == rhs.columnNames }
        public func hash(into hasher: inout Hasher) {
            hasher.combine(name)
            hasher.combine(unique)
            hasher.combine(columnNames)
        }
    
        private static let parserIgnoredCharacters: CharacterSet = .whitespacesAndNewlines.union(CharacterSet(charactersIn: "`'\""))

        internal let name: String
        internal let unique: Bool
        internal let columnNames: [String]
        
        internal func definition(tableName: String) -> String {
            if columnNames.isEmpty { fatalError("Indexes require at least one column") }
            return "CREATE \(unique ? "UNIQUE " : "")INDEX IF NOT EXISTS \(name) ON \(tableName) (\(columnNames.joined(separator: ",")))"
        }
        
        /// Defines a single [SQLite index](https://www.sqlite.org/lang_createindex.html) in a ``Blackbird/Table``.
        /// - Parameters:
        ///   - columnNames: An array of strings of the column names to index, in order.
        ///   - unique: Whether this index requires values to be unique. `NULL` values are exempt from the uniqueness requirement.
        public init(columnNames: [String], unique: Bool = false) {
            guard !columnNames.isEmpty else { fatalError("No columns specified") }
            self.columnNames = columnNames
            self.unique = unique
            self.name = columnNames.joined(separator: "_")
        }
        
        internal init(definition: String) throws {
            let scanner = Scanner(string: definition)
            scanner.charactersToBeSkipped = Self.parserIgnoredCharacters
            scanner.caseSensitive = false
            guard scanner.scanString("CREATE") != nil else { throw Error.cannotParseIndexDefinition(definition: definition, description: "Expected 'CREATE'") }
            unique = scanner.scanString("UNIQUE") != nil
            guard scanner.scanString("INDEX") != nil else { throw Error.cannotParseIndexDefinition(definition: definition, description: "Expected 'INDEX'") }

            guard let indexName = scanner.scanUpToString("ON")?.trimmingCharacters(in: Self.parserIgnoredCharacters), !indexName.isEmpty else {
                throw Error.cannotParseIndexDefinition(definition: definition, description: "Expected index name")
            }
            self.name = indexName
            guard scanner.scanString("ON") != nil else { throw Error.cannotParseIndexDefinition(definition: definition, description: "Expected 'ON'") }

            guard let tableName = scanner.scanUpToString("(")?.trimmingCharacters(in: Self.parserIgnoredCharacters), !tableName.isEmpty else {
                throw Error.cannotParseIndexDefinition(definition: definition, description: "Expected table name")
            }
            guard scanner.scanString("(") != nil, let columnList = scanner.scanUpToString(")"), scanner.scanString(")") != nil, !columnList.isEmpty else {
                throw Error.cannotParseIndexDefinition(definition: definition, description: "Expected column list")
            }
            
            columnNames = columnList.components(separatedBy: ",").map { $0.trimmingCharacters(in: Self.parserIgnoredCharacters) }.filter { !$0.isEmpty }
            guard !columnNames.isEmpty else { throw Error.cannotParseIndexDefinition(definition: definition, description: "No columns specified") }
        }
    }

    /// The schema for a SQLite table in a ``Database``.
    public struct Table: Hashable {
        enum Error: Swift.Error {
            case invalidTableDefinition(table: String, description: String)
        }
    
        public func hash(into hasher: inout Hasher) {
            hasher.combine(customName)
            hasher.combine(columns)
            hasher.combine(indexes)
            hasher.combine(primaryKeys)
        }

        internal func name<T>(type: T.Type) -> String {
            if let customName { return customName }
            return String(describing: type)
        }
        
        private let customName: String?
        internal let columns: [Column]
        internal let columnNames: Set<String>
        internal let primaryKeys: [Column]
        internal let indexes: [Index]
        
        private static var resolvedTablesWithDatabases: [Table: Set<Database.InstanceID>] = [:]
        private static var resolvedTableNamesInDatabases: [Database.InstanceID : Set<String>] = [:]
        private static var resolvedTablesLock = Lock()
        
        /// Defines the schema of an SQLite table in a ``Blackbird/Database`` for a type conforming to ``BlackbirdModel``.
        /// - Parameters:
        ///   - name: A custom name for the table.
        ///
        ///       **Default:** The ``BlackbirdModel``-conforming type's name.
        ///   - columns: An array of ``Blackbird/Column`` definitions. Must not be empty.
        ///   - primaryKeyColumnNames: An array of column names to define the primary key.
        ///
        ///       **Default:** `["id"]`
        ///   - indexes: An array of ``Blackbird/Index`` definitions for any additional indexed columns. The primary key is implicitly indexed and should not be included here.
        public init(name: String? = nil, columns: [Column], primaryKeyColumnNames: [String] = ["id"], indexes: [Index] = []) {
            if columns.isEmpty { fatalError("No columns specified") }
            
            self.customName = name
            self.columns = columns
            self.indexes = indexes
            self.columnNames = Set(columns.map { $0.name })
            self.primaryKeys = primaryKeyColumnNames.map { name in
                guard let pkColumn = columns.first(where: { $0.name == name }) else { fatalError("Primary-key column \"\(name)\" not found") }
                return pkColumn
            }
        }
        
        internal init?(isolatedCore core: isolated Database.Core, tableName: String) throws {
            if tableName.isEmpty { throw Error.invalidTableDefinition(table: tableName, description: "Table name cannot be empty") }

            var columns: [Column] = []
            var primaryKeyColumns: [Column] = []
            let query = "PRAGMA table_info('\(tableName)')"
            for row in try core.query(query) {
                let column = try Column(row: row, tableName: tableName)
                columns.append(column)
                if column.primaryKeyIndex > 0 { primaryKeyColumns.append(column) }
            }
            if columns.isEmpty { return nil }
            primaryKeyColumns.sort { $0.primaryKeyIndex < $1.primaryKeyIndex }
            
            self.customName = tableName
            self.columns = columns
            self.primaryKeys = primaryKeyColumns
            self.columnNames = Set(columns.map { $0.name })
            self.indexes = try core.query("SELECT sql FROM sqlite_master WHERE type = 'index' AND tbl_name = ?", tableName).compactMap { row in
                guard let sql = row["sql"]?.stringValue else { return nil }
                return try Index(definition: sql)
            }
        }
        
        internal func createTableStatement<T>(type: T.Type, tableName: String? = nil) -> String {
            let columnDefs = columns.map { $0.definition() }.joined(separator: ",")
            let pkDef = primaryKeys.isEmpty ? "" : ",PRIMARY KEY (`\(primaryKeys.map { $0.name }.joined(separator: "`,`"))`)"
            return "CREATE TABLE \(tableName ?? name(type: type)) (\(columnDefs)\(pkDef))"
        }
        
        internal func createIndexStatements<T>(type: T.Type) -> [String] { indexes.map { $0.definition(tableName: name(type: type)) } }
        
        internal func resolveWithDatabase<T>(type: T.Type, database: Database, core: Database.Core, validator: (() throws -> Void)?) async throws {
            if _isAlreadyResolved(type: type, in: database) { return }
            try await core.transaction {
                try _resolveWithDatabaseIsolated(type: type, database: database, core: $0, validator: validator)
            }
        }

        internal func resolveWithDatabaseIsolated<T>(type: T.Type, database: Database, core: isolated Database.Core, validator: (() throws -> Void)?) throws {
            if _isAlreadyResolved(type: type, in: database) { return }
            try _resolveWithDatabaseIsolated(type: type, database: database, core: core, validator: validator)
        }

        internal func _isAlreadyResolved<T>(type: T.Type, in database: Database) -> Bool {
            return Self.resolvedTablesLock.withLock {
                let alreadyResolved = Self.resolvedTablesWithDatabases[self]?.contains(database.id) ?? false
                if !alreadyResolved, case let name = name(type: type), Self.resolvedTableNamesInDatabases[database.id]?.contains(name) ?? false {
                    fatalError("Multiple BlackbirdModel types cannot use the same table name (\"\(name)\") in one Database")
                }
                return alreadyResolved
            }
        }

        private func _resolveWithDatabaseIsolated<T>(type: T.Type, database: Database, core: isolated Database.Core, validator: (() throws -> Void)?) throws {
            // Table not created yet
            var schemaInDB: Table
            let tableName = name(type: type)
            do {
                let existingSchemaInDB = try Table(isolatedCore: core, tableName: tableName)
                if let existingSchemaInDB {
                    schemaInDB = existingSchemaInDB
                } else {
                    try core.execute(createTableStatement(type: type))
                    for createIndexStatement in createIndexStatements(type: type) { try core.execute(createIndexStatement) }
                    schemaInDB = try Table(isolatedCore: core, tableName: tableName)!
                }
            }

            let primaryKeysChanged = (primaryKeys != schemaInDB.primaryKeys)
            
            // comparing as Sets to ignore differences in column/index order
            let currentColumns = Set(schemaInDB.columns)
            let targetColumns = Set(columns)
            let currentIndexes = Set(schemaInDB.indexes)
            let targetIndexes = Set(indexes)
            if primaryKeysChanged || currentColumns != targetColumns || currentIndexes != targetIndexes {
                try core.transaction { core in
                    // drop indexes and columns
                    for indexToDrop in currentIndexes.subtracting(targetIndexes) { try core.execute("DROP INDEX `\(indexToDrop.name)`") }
                    for columnNameToDrop in schemaInDB.columnNames.subtracting(columnNames) { try core.execute("ALTER TABLE `\(tableName)` DROP COLUMN `\(columnNameToDrop)`") }
                    schemaInDB = try Table(isolatedCore: core, tableName: tableName)!
                    
                    if primaryKeysChanged || !Set(schemaInDB.columns).subtracting(columns).isEmpty {
                        // At least one column has changed type -- do a full rebuild
                        let tempTableName = "_\(tableName)__\(Int32.random(in: 0..<Int32.max))"
                        try core.execute(createTableStatement(type: type, tableName: tempTableName))

                        let commonColumnNames = Set(schemaInDB.columnNames).intersection(columnNames)
                        let commonColumnsOrderedNameList = schemaInDB.columns.filter { commonColumnNames.contains($0.name) }.map { $0.name }
                        if !commonColumnsOrderedNameList.isEmpty {
                            let fieldList = "`\(commonColumnsOrderedNameList.joined(separator: "`,`"))`"
                            try core.execute("INSERT INTO `\(tempTableName)` (\(fieldList)) SELECT \(fieldList) FROM `\(tableName)`")
                        }
                        try core.execute("DROP TABLE `\(tableName)`")
                        try core.execute("ALTER TABLE `\(tempTableName)` RENAME TO `\(tableName)`")
                        schemaInDB = try Table(isolatedCore: core, tableName: tableName)!
                    }

                    // add columns and indexes
                    for columnToAdd in Set(columns).subtracting(schemaInDB.columns) { try core.execute("ALTER TABLE `\(tableName)` ADD COLUMN \(columnToAdd.definition())") }
                    for indexToAdd in Set(indexes).subtracting(schemaInDB.indexes) { try core.execute(indexToAdd.definition(tableName: tableName)) }

                    // allow calling model to verify before committing
                    if let validator { try validator() }
                }
            }
            
            Self.resolvedTablesLock.lock()
            if Self.resolvedTablesWithDatabases[self] == nil { Self.resolvedTablesWithDatabases[self] = Set<Database.InstanceID>() }
            Self.resolvedTablesWithDatabases[self]!.insert(database.id)
            if Self.resolvedTableNamesInDatabases[database.id] == nil { Self.resolvedTableNamesInDatabases[database.id] = Set<String>() }
            Self.resolvedTableNamesInDatabases[database.id]!.insert(tableName)
            Self.resolvedTablesLock.unlock()
        }
    }
}
