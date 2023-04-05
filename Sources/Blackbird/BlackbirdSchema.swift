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

    internal enum ColumnType: Sendable {
        case integer
        case double
        case text
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

    internal struct Column: Equatable, Hashable, Sendable {
        enum Error: Swift.Error {
            case cannotParseColumnDefinition(table: String, description: String)
        }
    
        // intentionally ignoring primaryKeyIndex since it's only used for internal sorting
        public static func == (lhs: Self, rhs: Self) -> Bool { lhs.name == rhs.name && lhs.columnType == rhs.columnType && lhs.mayBeNull == rhs.mayBeNull }
        public func hash(into hasher: inout Hasher) {
            hasher.combine(name)
            hasher.combine(columnType)
            hasher.combine(mayBeNull)
        }
    
        internal let name: String
        internal let columnType: ColumnType
        internal let valueType: Any.Type?
        internal let mayBeNull: Bool
        
        internal let primaryKeyIndex: Int // Only used for sorting, not considered for equality
        
        internal func definition() -> String {
            "`\(name)` \(columnType.definition()) \(mayBeNull ? "NULL" : "NOT NULL") DEFAULT \((mayBeNull ? .null : columnType.defaultValue()).sqliteLiteral())"
        }
                
        public init(name: String, columnType: ColumnType, valueType: Any.Type, mayBeNull: Bool = false) {
            self.name = name
            self.columnType = columnType
            self.valueType = valueType
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
            
            guard let columnType = ColumnType.parseType(typeStr) else { throw Error.cannotParseColumnDefinition(table: tableName, description: "Column \"\(name)\" has unsupported type: \"\(typeStr)\"") }
            self.name = name
            self.columnType = columnType
            self.valueType = nil
            self.mayBeNull = !notNull
            self.primaryKeyIndex = primaryKeyIndex
        }
    }
    
    internal struct Index: Equatable, Hashable, Sendable {
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
            return "CREATE \(unique ? "UNIQUE " : "")INDEX \(tableName)__\(name) ON \(tableName) (\(columnNames.joined(separator: ",")))"
        }
        
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

            guard let indexName = scanner.scanUpToString(" ON")?.trimmingCharacters(in: Self.parserIgnoredCharacters), !indexName.isEmpty else {
                throw Error.cannotParseIndexDefinition(definition: definition, description: "Expected index name")
            }

            let nameScanner = Scanner(string: indexName)
            _ = nameScanner.scanUpToString("__")
            if nameScanner.scanString("__") == "__" {
                self.name = String(indexName.suffix(from: nameScanner.currentIndex))
            } else {
                throw Error.cannotParseIndexDefinition(definition: definition, description: "Index name does not match expected format")
            }
            
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

    internal struct Table: Hashable, Sendable {
        static func == (lhs: Blackbird.Table, rhs: Blackbird.Table) -> Bool {
            lhs.name == rhs.name && lhs.columns == rhs.columns && lhs.indexes == rhs.indexes && lhs.primaryKeys == rhs.primaryKeys
        }
        
        enum Error: Swift.Error {
            case invalidTableDefinition(table: String, description: String)
            case impossibleMigration(table: String, description: String)
        }
    
        public func hash(into hasher: inout Hasher) {
            hasher.combine(name)
            hasher.combine(columns)
            hasher.combine(indexes)
            hasher.combine(primaryKeys)
        }
        
        internal let name: String
        internal let columns: [Column]
        internal let columnNames: ColumnNames
        internal let primaryKeys: [Column]
        internal let indexes: [Index]
        internal let upsertClause: String
        
        internal let emptyInstance: (any BlackbirdModel)?
        
        private static let resolvedTablesWithDatabases = Locked([Table: Set<Database.InstanceID>]())
        private static let resolvedTableNamesInDatabases = Locked([Database.InstanceID: Set<String>]())
        
        public init(name: String, columns: [Column], primaryKeyColumnNames: [String] = ["id"], indexes: [Index] = [], emptyInstance: any BlackbirdModel) {
            if columns.isEmpty { fatalError("No columns specified") }
            let orderedColumnNames = columns.map { $0.name }
            self.emptyInstance = emptyInstance
            self.name = name
            self.columns = columns
            self.indexes = indexes
            self.columnNames = Set(orderedColumnNames)
            self.primaryKeys = primaryKeyColumnNames.map { name in
                guard let pkColumn = columns.first(where: { $0.name == name }) else { fatalError("Primary-key column \"\(name)\" not found") }
                return pkColumn
            }
            
            upsertClause = Self.generateUpsertClause(columnNames: orderedColumnNames, primaryKeyColumnNames: primaryKeyColumnNames)
        }
        
        // Enable "upsert" (REPLACE INTO) behavior ONLY for primary-key conflicts, not any other UNIQUE constraints
        private static func generateUpsertClause(columnNames: [String], primaryKeyColumnNames: [String]) -> String {
            let upsertReplacements = columnNames.filter { !primaryKeyColumnNames.contains($0) }.map { "`\($0)` = excluded.`\($0)`" }
            return upsertReplacements.isEmpty ? "" : "ON CONFLICT (`\(primaryKeyColumnNames.joined(separator: "`,`"))`) DO UPDATE SET \(upsertReplacements.joined(separator: ","))"
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
            let orderedColumnNames = columns.map { $0.name }
            
            self.emptyInstance = nil
            self.name = tableName
            self.columns = columns
            self.primaryKeys = primaryKeyColumns
            self.columnNames = Set(orderedColumnNames)
            self.indexes = try core.query("SELECT sql FROM sqlite_master WHERE type = 'index' AND tbl_name = ?", tableName).compactMap { row in
                guard let sql = row["sql"]?.stringValue else { return nil }
                return try Index(definition: sql)
            }

            upsertClause = Self.generateUpsertClause(columnNames: orderedColumnNames, primaryKeyColumnNames: primaryKeyColumns.map { $0.name })
        }

        internal func keyPathToColumnInfo(keyPath: AnyKeyPath) -> Blackbird.ColumnInfo {
            guard let emptyInstance else { fatalError("Cannot call keyPathToColumnName on a Blackbird.Table initialized directly from a database") }
            guard let column = emptyInstance[keyPath: keyPath] as? any ColumnWrapper else { fatalError("Key path is not a @BlackbirdColumn on \(name)") }
            guard let name = column.internalNameInSchemaGenerator.value else { fatalError("Failed to look up key-path name on \(name)") }
            return Blackbird.ColumnInfo(name: name, type: column.valueType.self)
        }

        internal func keyPathToColumnName(keyPath: AnyKeyPath) -> String {
            guard let emptyInstance else { fatalError("Cannot call keyPathToColumnName on a Blackbird.Table initialized directly from a database") }
            guard let column = emptyInstance[keyPath: keyPath] as? any ColumnWrapper else { fatalError("Key path is not a @BlackbirdColumn on \(name). Make sure to use the $-prefixed wrapper, e.g. \\.$id.") }
            guard let name = column.internalNameInSchemaGenerator.value else { fatalError("Failed to look up key-path name on \(name)") }
            return name
        }

        internal func createTableStatement<T: BlackbirdModel>(type: T.Type, overrideTableName: String? = nil) -> String {
            let columnDefs = columns.map { $0.definition() }.joined(separator: ",")
            let pkDef = primaryKeys.isEmpty ? "" : ",PRIMARY KEY (`\(primaryKeys.map { $0.name }.joined(separator: "`,`"))`)"
            return "CREATE TABLE \(overrideTableName ?? name) (\(columnDefs)\(pkDef))"
        }
        
        internal func createIndexStatements<T: BlackbirdModel>(type: T.Type) -> [String] { indexes.map { $0.definition(tableName: name) } }
        
        internal func resolveWithDatabase<T: BlackbirdModel>(type: T.Type, database: Database, core: Database.Core, validator: (@Sendable () throws -> Void)?) async throws {
            if _isAlreadyResolved(type: type, in: database) { return }
            try await core.transaction {
                try _resolveWithDatabaseIsolated(type: type, database: database, core: $0, validator: validator)
            }
        }

        internal func resolveWithDatabaseIsolated<T: BlackbirdModel>(type: T.Type, database: Database, core: isolated Database.Core, validator: (@Sendable () throws -> Void)?) throws {
            if _isAlreadyResolved(type: type, in: database) { return }
            try _resolveWithDatabaseIsolated(type: type, database: database, core: core, validator: validator)
        }

        internal func _isAlreadyResolved<T>(type: T.Type, in database: Database) -> Bool {
            let alreadyResolved = Self.resolvedTablesWithDatabases.withLock { $0[self]?.contains(database.id) ?? false }
            if !alreadyResolved, Self.resolvedTableNamesInDatabases.withLock({ $0[database.id]?.contains(name) ?? false }) {
                fatalError("Multiple BlackbirdModel types cannot use the same table name (\"\(name)\") in one Database")
            }
            return alreadyResolved
        }

        private func _resolveWithDatabaseIsolated<T: BlackbirdModel>(type: T.Type, database: Database, core: isolated Database.Core, validator: (@Sendable () throws -> Void)?) throws {
            // Table not created yet
            let schemaInDB: Table
            do {
                let existingSchemaInDB = try Table(isolatedCore: core, tableName: name)
                if let existingSchemaInDB {
                    schemaInDB = existingSchemaInDB
                } else {
                    try core.execute(createTableStatement(type: type))
                    for createIndexStatement in createIndexStatements(type: type) { try core.execute(createIndexStatement) }
                    schemaInDB = try Table(isolatedCore: core, tableName: name)!
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
                    var schemaInDB = schemaInDB
                    for indexToDrop in currentIndexes.subtracting(targetIndexes) { try core.execute("DROP INDEX `\(name)__\(indexToDrop.name)`") }
                    for columnNameToDrop in schemaInDB.columnNames.subtracting(columnNames) { try core.execute("ALTER TABLE `\(name)` DROP COLUMN `\(columnNameToDrop)`") }
                    schemaInDB = try Table(isolatedCore: core, tableName: name)!
                    
                    if primaryKeysChanged || !Set(schemaInDB.columns).subtracting(columns).isEmpty {
                        // At least one column has changed type -- do a full rebuild
                        let tempTableName = "_\(name)__\(Int32.random(in: 0..<Int32.max))"
                        try core.execute(createTableStatement(type: type, overrideTableName: tempTableName))

                        let commonColumnNames = Set(schemaInDB.columnNames).intersection(columnNames)
                        let commonColumnsOrderedNameList = schemaInDB.columns.filter { commonColumnNames.contains($0.name) }.map { $0.name }
                        if !commonColumnsOrderedNameList.isEmpty {
                            let fieldList = "`\(commonColumnsOrderedNameList.joined(separator: "`,`"))`"
                            try core.execute("INSERT INTO `\(tempTableName)` (\(fieldList)) SELECT \(fieldList) FROM `\(name)`")
                        }
                        try core.execute("DROP TABLE `\(name)`")
                        try core.execute("ALTER TABLE `\(tempTableName)` RENAME TO `\(name)`")
                        schemaInDB = try Table(isolatedCore: core, tableName: name)!
                    }

                    // add columns and indexes
                    for columnToAdd in Set(columns).subtracting(schemaInDB.columns) {
                        if !columnToAdd.mayBeNull, let valueType = columnToAdd.valueType, valueType is URL.Type {
                            throw Error.impossibleMigration(
                                table: name,
                                description: "Cannot add non-NULL URL column `\(columnToAdd.name)` since default values for existing rows cannot be specified"
                            )
                        }
                        
                        try core.execute("ALTER TABLE `\(name)` ADD COLUMN \(columnToAdd.definition())")
                    }
                    
                    for indexToAdd in Set(indexes).subtracting(schemaInDB.indexes) { try core.execute(indexToAdd.definition(tableName: name)) }

                    // allow calling model to verify before committing
                    if let validator { try validator() }
                }
            }

            Self.resolvedTablesWithDatabases.withLock {
                if $0[self] == nil { $0[self] = Set<Database.InstanceID>() }
                $0[self]!.insert(database.id)
            }
            
            Self.resolvedTableNamesInDatabases.withLock {
                if $0[database.id] == nil { $0[database.id] = Set<String>() }
                $0[database.id]!.insert(name)
            }
        }
    }
}


internal extension String {
    func removingLeadingUnderscore() -> String {
        guard self.hasPrefix("_"), self.count > 1, let firstCharIndex = self.indices.first else { return self }
        return String(self.suffix(from: self.index(after: firstCharIndex)))
    }
}

internal final class SchemaGenerator: Sendable {
    internal static let shared = SchemaGenerator()
    
    let tableCache = Blackbird.Locked<[ObjectIdentifier: Blackbird.Table]>([:])
    
    internal func table<T: BlackbirdModel>(for type: T.Type) -> Blackbird.Table {
        tableCache.withLock { cache in
            let identifier = ObjectIdentifier(type)
            if let cached = cache[identifier] { return cached }

            let table = Self.generateTableDefinition(type)
            cache[identifier] = table
            return table
        }
    }

    internal static func instanceFromDefaults<T>(_ type: T.Type) -> T where T: BlackbirdModel {
        do {
            return try T(from: EmptyDecoder())
        } catch {
            fatalError("\(String(describing: T.self)) instances cannot be generated by simple decoding: \(error)")
        }
    }

    private static func generateTableDefinition<T>(_ type: T.Type) -> Blackbird.Table where T: BlackbirdModel {
        let emptyInstance = instanceFromDefaults(type)
        
        let mirror = Mirror(reflecting: emptyInstance)
        var columns: [Blackbird.Column] = []
        var nullableColumnNames = Set<String>()
        var hasColumNamedID = false
        for child in mirror.children {
            guard var label = child.label else { continue }

            if let column = child.value as? any ColumnWrapper {
                label = label.removingLeadingUnderscore()
                column.internalNameInSchemaGenerator.value = label
                if label == "id" { hasColumNamedID = true }

                var isOptional = false
                var unwrappedType = Swift.type(of: column.value) as Any.Type
                while let wrappedType = unwrappedType as? WrappedType.Type {
                    if unwrappedType is OptionalProtocol.Type {
                        isOptional = true
                        nullableColumnNames.insert(label)
                    }
                    unwrappedType = wrappedType.schemaGeneratorWrappedType()
                }

                var columnType: Blackbird.ColumnType
                switch unwrappedType {
                    case is BlackbirdStorableAsInteger.Type: columnType = .integer
                    case is BlackbirdStorableAsDouble.Type:  columnType = .double
                    case is BlackbirdStorableAsText.Type:    columnType = .text
                    case is BlackbirdStorableAsData.Type:    columnType = .data
                    case is any BlackbirdIntegerEnum.Type:   columnType = .integer
                    case is any BlackbirdStringEnum.Type:    columnType = .text
                    default:
                        fatalError("\(String(describing: T.self)).\(label) is not a supported type for a database column (\(String(describing: unwrappedType)))")
                }
                
                columns.append(Blackbird.Column(name: label, columnType: columnType, valueType: unwrappedType, mayBeNull: isOptional))
            }
        }
        
        let keyPathToColumnName = { (keyPath: AnyKeyPath, messageLabel: String) in
            guard let column = emptyInstance[keyPath: keyPath] as? any ColumnWrapper else {
                fatalError("\(String(describing: T.self)): \(messageLabel) includes a key path that is not a @BlackbirdColumn. (Use the \"$\" wrapper for a column.)")
            }
            
            guard let name = column.internalNameInSchemaGenerator.value else { fatalError("\(String(describing: T.self)): Failed to look up \(messageLabel) key-path name") }
            return name
        }
                
        var primaryKeyNames = T.primaryKey.map { keyPathToColumnName($0, "primary key") }
        if primaryKeyNames.count == 0 {
            if hasColumNamedID { primaryKeyNames = ["id"] }
            else { fatalError("\(String(describing: T.self)): Must specify a primary key or have a property named \"id\" to automatically use as primary key") }
        }

        var indexes = T.indexes.map { keyPaths in Blackbird.Index(columnNames: keyPaths.map { keyPathToColumnName($0, "index") }, unique: false) }
        indexes.append(contentsOf: T.uniqueIndexes.map { keyPaths in
            Blackbird.Index(columnNames: keyPaths.map {
                let name = keyPathToColumnName($0, "unique index")
                if nullableColumnNames.contains(name), keyPaths.count > 1 {
                    /*
                        I've decided not to support multi-column UNIQUE indexes containing NULLable columns because
                        they behave in a way that most people wouldn't expect: a NULL value anywhere in a multi-column
                        index makes it pass any UNIQUE checks, even if the other column values would otherwise be
                        non-unique.
                        
                        E.g. CREATE TABLE t (a NOT NULL, b NULL) with UNIQUE (a, b) would allow these rows to coexist:

                           (a=1, b=NULL)
                           (a=1, b=NULL)
                           
                        ...even though they would not be considered unique values by Swift or most people's assumptions.
                        
                        Since Blackbird tries to abstract away most really weird SQL behaviors that would differ
                        significantly from what Swift programmers expect, this is intentionally not permitted.
                     */
                    fatalError(
                        "\(String(describing: T.self)): Blackbird does not support multi-column UNIQUE indexes containing NULL columns. " +
                        "Change column \"\(name)\" to non-optional, or create a separate UNIQUE index for it."
                    )
                }
                return name
            }, unique: true)
        })
        
        var indexedColumnSets = Set<[String]>()
        for index in indexes {
            let (inserted, _) = indexedColumnSets.insert(index.columnNames)
            if !inserted { fatalError("\(String(describing: T.self)): Duplicate index definitions for [\(index.columnNames.joined(separator: ","))]") }
        }

        return Blackbird.Table(name: T.tableName, columns: columns, primaryKeyColumnNames: primaryKeyNames, indexes: indexes, emptyInstance: emptyInstance)
    }
}

// MARK: - Empty initialization of Codable types
//
// HUGE credit to https://github.com/jjrscott/EmptyInitializer for this EmptyDecoder trick!
// The following is mostly a condensed version of that code with minor tweaks,
//  mostly to work with BlackbirdColumn wrappers and support URL as a property type.

fileprivate struct EmptyDecoder: Decoder {
    var codingPath: [CodingKey] = []
    var userInfo: [CodingUserInfoKey: Any] = [:]
    func container<Key>(keyedBy type: Key.Type) throws -> KeyedDecodingContainer<Key> where Key: CodingKey { KeyedDecodingContainer(EmptyKeyedDecodingContainer<Key>()) }
    func unkeyedContainer() throws -> UnkeyedDecodingContainer { EmptyUnkeyedDecodingContainer() }
    func singleValueContainer() throws -> SingleValueDecodingContainer { EmptySingleValueDecodingContainer() }
}

fileprivate struct EmptyKeyedDecodingContainer<Key: CodingKey>: KeyedDecodingContainerProtocol {
    var codingPath: [CodingKey] = []
    var allKeys: [Key] = []
    func contains(_ key: Key) -> Bool { true }
    func decodeNil(forKey key: Key) throws -> Bool { true }
    func decode(_ type: Bool.Type, forKey key: Key) throws -> Bool     { false }
    func decode(_ type: String.Type, forKey key: Key) throws -> String { "" }
    func decode(_ type: Double.Type, forKey key: Key) throws -> Double { 0 }
    func decode(_ type: Float.Type, forKey key: Key) throws -> Float   { 0 }
    func decode(_ type: Int.Type, forKey key: Key) throws -> Int       { 0 }
    func decode(_ type: Int8.Type, forKey key: Key) throws -> Int8     { 0 }
    func decode(_ type: Int16.Type, forKey key: Key) throws -> Int16   { 0 }
    func decode(_ type: Int32.Type, forKey key: Key) throws -> Int32   { 0 }
    func decode(_ type: Int64.Type, forKey key: Key) throws -> Int64   { 0 }
    func decode(_ type: UInt.Type, forKey key: Key) throws -> UInt     { 0 }
    func decode(_ type: UInt8.Type, forKey key: Key) throws -> UInt8   { 0 }
    func decode(_ type: UInt16.Type, forKey key: Key) throws -> UInt16 { 0 }
    func decode(_ type: UInt32.Type, forKey key: Key) throws -> UInt32 { 0 }
    func decode(_ type: UInt64.Type, forKey key: Key) throws -> UInt64 { 0 }
    func decode<T>(_ type: BlackbirdColumn<T>.Type, forKey key: Key) throws -> BlackbirdColumn<T> { BlackbirdColumn<T>(wrappedValue: try T(from: EmptyDecoder())) }
    func decode<T>(_ type: T.Type, forKey key: Key) throws -> T where T: Decodable {
        if T.self == URL.self { return URL(string: "file:///") as! T }
        if T.self == Data.self { return Data() as! T }
        if let iterableT = T.self as? any CaseIterable.Type, let first = (iterableT.allCases as any Collection).first { return first as! T }
        return try T(from: EmptyDecoder())
    }
    func nestedUnkeyedContainer(forKey key: Key) throws -> UnkeyedDecodingContainer { EmptyUnkeyedDecodingContainer() }
    func superDecoder() throws -> Decoder { EmptyDecoder() }
    func superDecoder(forKey key: Key) throws -> Decoder { EmptyDecoder() }
    func nestedContainer<NestedKey>(keyedBy type: NestedKey.Type, forKey key: Key) throws -> KeyedDecodingContainer<NestedKey> where NestedKey: CodingKey {
        KeyedDecodingContainer(EmptyKeyedDecodingContainer<NestedKey>())
    }
}

fileprivate struct EmptySingleValueDecodingContainer: SingleValueDecodingContainer {
    var codingPath: [CodingKey] = []
    func decodeNil() -> Bool { true }
    func decode(_ type: Bool.Type) throws -> Bool     { false }
    func decode(_ type: String.Type) throws -> String { "" }
    func decode(_ type: Double.Type) throws -> Double { 0 }
    func decode(_ type: Float.Type) throws -> Float   { 0 }
    func decode(_ type: Int.Type) throws -> Int       { 0 }
    func decode(_ type: Int8.Type) throws -> Int8     { 0 }
    func decode(_ type: Int16.Type) throws -> Int16   { 0 }
    func decode(_ type: Int32.Type) throws -> Int32   { 0 }
    func decode(_ type: Int64.Type) throws -> Int64   { 0 }
    func decode(_ type: UInt.Type) throws -> UInt     { 0 }
    func decode(_ type: UInt8.Type) throws -> UInt8   { 0 }
    func decode(_ type: UInt16.Type) throws -> UInt16 { 0 }
    func decode(_ type: UInt32.Type) throws -> UInt32 { 0 }
    func decode(_ type: UInt64.Type) throws -> UInt64 { 0 }
    func decode<T>(_ type: BlackbirdColumn<T>.Type) throws -> BlackbirdColumn<T> { BlackbirdColumn<T>(wrappedValue: try T(from: EmptyDecoder())) }
    func decode<T>(_ type: T.Type) throws -> T where T: Decodable {
        if T.self == URL.self { return URL(string: "file:///") as! T }
        if T.self == Data.self { return Data() as! T }
        if let iterableT = T.self as? any CaseIterable.Type, let first = (iterableT.allCases as any Collection).first { return first as! T }
        return try T(from: EmptyDecoder())
    }
}

fileprivate struct EmptyUnkeyedDecodingContainer: UnkeyedDecodingContainer {
    var codingPath: [CodingKey] = []
    var count: Int?
    var isAtEnd: Bool = true
    var currentIndex: Int = 0
    mutating func decodeNil() throws -> Bool { true }
    mutating func decode(_ type: Bool.Type) throws -> Bool     { false }
    mutating func decode(_ type: String.Type) throws -> String { "" }
    mutating func decode(_ type: Double.Type) throws -> Double { 0 }
    mutating func decode(_ type: Float.Type) throws -> Float   { 0 }
    mutating func decode(_ type: Int.Type) throws -> Int       { 0 }
    mutating func decode(_ type: Int8.Type) throws -> Int8     { 0 }
    mutating func decode(_ type: Int16.Type) throws -> Int16   { 0 }
    mutating func decode(_ type: Int32.Type) throws -> Int32   { 0 }
    mutating func decode(_ type: Int64.Type) throws -> Int64   { 0 }
    mutating func decode(_ type: UInt.Type) throws -> UInt     { 0 }
    mutating func decode(_ type: UInt8.Type) throws -> UInt8   { 0 }
    mutating func decode(_ type: UInt16.Type) throws -> UInt16 { 0 }
    mutating func decode(_ type: UInt32.Type) throws -> UInt32 { 0 }
    mutating func decode(_ type: UInt64.Type) throws -> UInt64 { 0 }
    mutating func decode<T>(_ type: BlackbirdColumn<T>.Type) throws -> BlackbirdColumn<T> { BlackbirdColumn<T>(wrappedValue: try T(from: EmptyDecoder())) }
    mutating func decode<T>(_ type: T.Type) throws -> T where T: Decodable {
        if T.self == URL.self { return URL(string: "file:///") as! T }
        if T.self == Data.self { return Data() as! T }
        if let iterableT = T.self as? any CaseIterable.Type, let first = (iterableT.allCases as any Collection).first { return first as! T }
        return try T(from: EmptyDecoder())
    }
    
    mutating func nestedUnkeyedContainer() throws -> UnkeyedDecodingContainer { EmptyUnkeyedDecodingContainer() }
    mutating func superDecoder() throws -> Decoder { EmptyDecoder() }
    mutating func nestedContainer<NestedKey>(keyedBy type: NestedKey.Type) throws -> KeyedDecodingContainer<NestedKey> where NestedKey: CodingKey {
        KeyedDecodingContainer(EmptyKeyedDecodingContainer<NestedKey>())
    }
}
