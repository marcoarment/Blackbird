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
            return "CREATE \(unique ? "UNIQUE " : "")INDEX IF NOT EXISTS \(name) ON \(tableName) (\(columnNames.joined(separator: ",")))"
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

    internal struct Table: Hashable, Sendable {
        enum Error: Swift.Error {
            case invalidTableDefinition(table: String, description: String)
        }
    
        public func hash(into hasher: inout Hasher) {
            hasher.combine(name)
            hasher.combine(columns)
            hasher.combine(indexes)
            hasher.combine(primaryKeys)
        }
        
        internal let name: String
        internal let columns: [Column]
        internal let columnNames: Set<String>
        internal let primaryKeys: [Column]
        internal let indexes: [Index]
        
        private static let resolvedTablesWithDatabases = Locked([Table: Set<Database.InstanceID>]())
        private static let resolvedTableNamesInDatabases = Locked([Database.InstanceID : Set<String>]())
        
        public init(name: String = "bogus", columns: [Column], primaryKeyColumnNames: [String] = ["id"], indexes: [Index] = []) {
            if columns.isEmpty { fatalError("No columns specified") }
            
            self.name = name
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
            
            self.name = tableName
            self.columns = columns
            self.primaryKeys = primaryKeyColumns
            self.columnNames = Set(columns.map { $0.name })
            self.indexes = try core.query("SELECT sql FROM sqlite_master WHERE type = 'index' AND tbl_name = ?", tableName).compactMap { row in
                guard let sql = row["sql"]?.stringValue else { return nil }
                return try Index(definition: sql)
            }
        }
        
        internal func createTableStatement<T>(type: T.Type, overrideTableName: String? = nil) -> String {
            let columnDefs = columns.map { $0.definition() }.joined(separator: ",")
            let pkDef = primaryKeys.isEmpty ? "" : ",PRIMARY KEY (`\(primaryKeys.map { $0.name }.joined(separator: "`,`"))`)"
            return "CREATE TABLE \(overrideTableName ?? name) (\(columnDefs)\(pkDef))"
        }
        
        internal func createIndexStatements<T>(type: T.Type) -> [String] { indexes.map { $0.definition(tableName: name) } }
        
        internal func resolveWithDatabase<T>(type: T.Type, database: Database, core: Database.Core, validator: (@Sendable () throws -> Void)?) async throws {
            if _isAlreadyResolved(type: type, in: database) { return }
            try await core.transaction {
                try _resolveWithDatabaseIsolated(type: type, database: database, core: $0, validator: validator)
            }
        }

        internal func resolveWithDatabaseIsolated<T>(type: T.Type, database: Database, core: isolated Database.Core, validator: (@Sendable () throws -> Void)?) throws {
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

        private func _resolveWithDatabaseIsolated<T>(type: T.Type, database: Database, core: isolated Database.Core, validator: (@Sendable () throws -> Void)?) throws {
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
                    for indexToDrop in currentIndexes.subtracting(targetIndexes) { try core.execute("DROP INDEX `\(indexToDrop.name)`") }
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
                    for columnToAdd in Set(columns).subtracting(schemaInDB.columns) { try core.execute("ALTER TABLE `\(name)` ADD COLUMN \(columnToAdd.definition())") }
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

internal protocol ColumnWrapper: WrappedType {
    associatedtype ValueType: Codable & Hashable & Sendable
    var value: ValueType { get }
    var internalNameInSchemaGenerator: String? { get set }
}


// Keeping this one @unchecked-Sendable for now to see if locking around .value is really necessary in practice.
// I know this is wrong and I apologize to the world as necessary.
@propertyWrapper public struct BlackbirdColumn<T>: ColumnWrapper, WrappedType, Equatable, @unchecked Sendable, Codable where T: BlackbirdColumnWrappable {
    public static func == (lhs: Self, rhs: Self) -> Bool { type(of: lhs) == type(of: rhs) && lhs.value == rhs.value }

    internal final class StringClassWrapper: @unchecked Sendable {
        var value: String? = nil
    }
    
    internal var _internalNameInSchemaGenerator = StringClassWrapper()
    var internalNameInSchemaGenerator: String? {
        get { _internalNameInSchemaGenerator.value }
        set { _internalNameInSchemaGenerator.value = newValue }
    }

    internal var value: T

    public var projectedValue: BlackbirdColumn<T> { self }
    static internal func schemaGeneratorWrappedType() -> Any.Type { T.self }

    public var wrappedValue: T {
        get { value }
        set { value = newValue }
    }

    public init(wrappedValue: T) { self.value = wrappedValue }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        value = try container.decode(T.self)
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        try container.encode(value)
    }
}

internal final class SchemaGenerator: Sendable {
    internal static let shared = SchemaGenerator()
    
    let tableCache = Blackbird.Locked<[ObjectIdentifier : Blackbird.Table]>([:])
    
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
        var hasColumNamedID = false
        for child in mirror.children {
            guard var label = child.label else { continue }

            if var column = child.value as? any ColumnWrapper {
                // remove the "_" preceding internal names of property-wrapped values
                guard label.hasPrefix("_"), label.count > 1, let firstCharIndex = label.indices.first else {
                    fatalError("\(String(describing: T.self)).\(label): cannot parse label format, expected e.g. \"_name\"")
                }
                label.remove(at: firstCharIndex)
                
                column.internalNameInSchemaGenerator = label
                if label == "id" { hasColumNamedID = true }

                var isOptional = false
                var unwrappedType = Swift.type(of: column.value) as Any
                while let wrappedType = unwrappedType as? WrappedType.Type {
                    if unwrappedType is OptionalProtocol.Type { isOptional = true }
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
                
                columns.append(Blackbird.Column(name: label, type: columnType, mayBeNull: isOptional))
            }
        }
        
        let keyPathToColumnName = { (keyPath: AnyKeyPath, messageLabel: String) in
            guard let column = emptyInstance[keyPath: keyPath] as? any ColumnWrapper else {
                fatalError("\(String(describing: T.self)): \(messageLabel) includes a key path that is not a @BlackbirdColumn")
            }
            
            guard let name = column.internalNameInSchemaGenerator else { fatalError("\(String(describing: T.self)): Failed to look up \(messageLabel) key-path name") }
            return name
        }
                
        var primaryKeyNames = T.primaryKey.map { keyPathToColumnName($0, "primary key") }
        if primaryKeyNames.count == 0 {
            if hasColumNamedID { primaryKeyNames = ["id"] }
            else { fatalError("\(String(describing: T.self)): Must specify a primary key or have a property named \"id\" to automatically use as primary key") }
        }

        var indexes = T.indexes.map { keyPaths in Blackbird.Index(columnNames: keyPaths.map { keyPathToColumnName($0, "index") }, unique: false) }
        indexes.append(contentsOf: T.uniqueIndexes.map { keyPaths in Blackbird.Index(columnNames: keyPaths.map { keyPathToColumnName($0, "unique index") }, unique: true) })

        return Blackbird.Table(name: T.tableName, columns: columns, primaryKeyColumnNames: primaryKeyNames, indexes: indexes)
    }
}

// MARK: - Accessing wrapped types of optionals and column wrappers
fileprivate protocol OptionalProtocol { }
extension Optional: OptionalProtocol { }

internal protocol WrappedType {
    static func schemaGeneratorWrappedType() -> Any.Type
}

extension Optional: WrappedType {
    internal static func schemaGeneratorWrappedType() -> Any.Type { Wrapped.self }
}

// MARK: - Empty initialization of Codable types
//
// HUGE credit to https://github.com/jjrscott/EmptyInitializer for this EmptyDecoder trick!
// The following is mostly a condensed version of that code with minor tweaks,
//  mostly to work with BlackbirdColumn wrappers and support URL as a property type.

fileprivate struct EmptyDecoder: Decoder {
    var codingPath: [CodingKey] = []
    var userInfo: [CodingUserInfoKey: Any] = [:]
    func container<Key>(keyedBy type: Key.Type) throws -> KeyedDecodingContainer<Key> where Key : CodingKey { KeyedDecodingContainer(EmptyKeyedDecodingContainer<Key>()) }
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
