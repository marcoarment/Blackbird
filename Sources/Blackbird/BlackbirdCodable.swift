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
//  BlackbirdCodable.swift
//  Created by Marco Arment on 11/7/22.
//
//  With significant thanks to (and borrowing from):
//   https://shareup.app/blog/encoding-and-decoding-sqlite-in-swift/
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

internal class BlackbirdSQLiteDecoder: Decoder {
    public enum Error: Swift.Error {
        case invalidValue(String, value: String)
        case missingValue(String)
    }

    public var codingPath: [CodingKey] = []
    public var userInfo: [CodingUserInfoKey: Any] = [:]

    let database: Blackbird.Database
    let row: Blackbird.Row
    init(database: Blackbird.Database, row: Blackbird.Row, codingPath: [CodingKey] = []) {
        self.database = database
        self.row = row
        self.codingPath = codingPath
    }

    public func container<Key>(keyedBy type: Key.Type) throws -> KeyedDecodingContainer<Key> where Key: CodingKey {
        if let iterableKey = Key.self as? any BlackbirdCodingKey.Type {
            // Custom CodingKeys are in use, so remap the row to use the expected keys instead of raw column names
            var newRow = Blackbird.Row()
            for (columnName, customFieldName) in iterableKey.allLabeledCases {
                if let rowValue = row[columnName] {
                    newRow[customFieldName] = rowValue
                }
            }
            return KeyedDecodingContainer(BlackbirdSQLiteKeyedDecodingContainer<Key>(codingPath: codingPath, database: database, row: newRow))
        }
        
        // Use default names without custom CodingKeys
        return KeyedDecodingContainer(BlackbirdSQLiteKeyedDecodingContainer<Key>(codingPath: codingPath, database: database, row: row))
    }

    public func unkeyedContainer() throws -> UnkeyedDecodingContainer { fatalError("unsupported") }
    public func singleValueContainer() throws -> SingleValueDecodingContainer { BlackbirdSQLiteSingleValueDecodingContainer(codingPath: codingPath, database: database, row: row) }
}

fileprivate struct BlackbirdSQLiteSingleValueDecodingContainer: SingleValueDecodingContainer {
    public enum Error: Swift.Error {
        case invalidEnumValue(typeDescription: String, value: Any)
    }

    var codingPath: [CodingKey] = []
    let database: Blackbird.Database
    var row: Blackbird.Row
    
    init(codingPath: [CodingKey], database: Blackbird.Database, row: Blackbird.Row) {
        self.codingPath = codingPath
        self.database = database
        self.row = row
    }
    
    private func value() throws -> Blackbird.Value {
        guard let key = codingPath.first?.stringValue, let v = row[key] else {
            throw BlackbirdSQLiteDecoder.Error.missingValue(codingPath.first?.stringValue ?? "(unknown key)")
        }
        return v
    }

    func decodeNil() -> Bool { true }
    func decode(_ type: Bool.Type) throws -> Bool     { (try value()).boolValue ?? false }
    func decode(_ type: String.Type) throws -> String { (try value()).stringValue ?? "" }
    func decode(_ type: Double.Type) throws -> Double { (try value()).doubleValue ?? 0 }
    func decode(_ type: Float.Type) throws -> Float   { Float((try value()).doubleValue ?? 0) }
    func decode(_ type: Int.Type) throws -> Int       { (try value()).intValue ?? 0 }
    func decode(_ type: Int8.Type) throws -> Int8     { Int8((try value()).intValue ?? 0) }
    func decode(_ type: Int16.Type) throws -> Int16   { Int16((try value()).intValue ?? 0) }
    func decode(_ type: Int32.Type) throws -> Int32   { Int32((try value()).intValue ?? 0) }
    func decode(_ type: Int64.Type) throws -> Int64   { (try value()).int64Value ?? 0 }
    func decode(_ type: UInt.Type) throws -> UInt     { UInt((try value()).int64Value ?? 0) }
    func decode(_ type: UInt8.Type) throws -> UInt8   { UInt8((try value()).intValue ?? 0) }
    func decode(_ type: UInt16.Type) throws -> UInt16 { UInt16((try value()).intValue ?? 0) }
    func decode(_ type: UInt32.Type) throws -> UInt32 { UInt32((try value()).int64Value ?? 0) }
    func decode(_ type: UInt64.Type) throws -> UInt64 { UInt64((try value()).int64Value ?? 0) }

    func decode<T>(_ type: T.Type) throws -> T where T: Decodable {
        let value = try value()
        if T.self == Data.self { return (value.dataValue ?? Data()) as! T }
        if T.self == URL.self, let urlStr = value.stringValue, let url = URL(string: urlStr) { return url as! T }
        if T.self == Date.self { return Date(timeIntervalSince1970: value.doubleValue ?? 0) as! T }

        if let eT = T.self as? any BlackbirdIntegerOptionalEnum.Type, value.int64Value == nil {
            return (try decodeNilRepresentable(eT) as? T)!
        }

        if let eT = T.self as? any BlackbirdStringOptionalEnum.Type, value.stringValue == nil {
            return (try decodeNilRepresentable(eT) as? T)!
        }

        if let eT = T.self as? any BlackbirdIntegerEnum.Type {
            let rawValue = value.int64Value ?? 0
            guard let integerEnum = try decodeRepresentable(eT, unifiedRawValue: rawValue), let converted = integerEnum as? T else {
                throw Error.invalidEnumValue(typeDescription: String(describing: eT), value: rawValue)
            }
            return converted
        }

        if let eT = T.self as? any BlackbirdStringEnum.Type {
            let rawValue = value.stringValue ?? ""
            guard let stringEnum = try decodeRepresentable(eT, unifiedRawValue: rawValue), let converted = stringEnum as? T else {
                throw Error.invalidEnumValue(typeDescription: String(describing: eT), value: rawValue)
            }
            return converted
        }

        if let eT = T.self as? any OptionalCreatable.Type, let wrappedType = eT.creatableWrappedType() as? any Decodable.Type {
            if value == .null {
                return eT.createFromNilValue() as! T
            } else {
                let wrappedValue = try decode(wrappedType)
                return eT.createFromValue(wrappedValue) as! T
            }
        }

        if let eT = T.self as? any BlackbirdStorableAsData.Type, let data = value.dataValue {
            return try JSONDecoder().decode(eT, from: data) as! T
        }

        return try T(from: BlackbirdSQLiteDecoder(database: database, row: row, codingPath: codingPath))
    }

    func decodeRepresentable<T>(_ type: T.Type, unifiedRawValue: Int64) throws -> T? where T: BlackbirdIntegerEnum {
        T.init(rawValue: T.RawValue.from(unifiedRepresentation: unifiedRawValue))
    }

    func decodeRepresentable<T>(_ type: T.Type, unifiedRawValue: String) throws -> T? where T: BlackbirdStringEnum {
        T.init(rawValue: T.RawValue.from(unifiedRepresentation: unifiedRawValue))
    }

    func decodeNilRepresentable<T>(_ type: T.Type) throws -> T where T: BlackbirdIntegerOptionalEnum {
        T.nilInstance()
    }

    func decodeNilRepresentable<T>(_ type: T.Type) throws -> T where T: BlackbirdStringOptionalEnum {
        T.nilInstance()
    }
}

fileprivate class BlackbirdSQLiteKeyedDecodingContainer<K: CodingKey>: KeyedDecodingContainerProtocol {
    typealias Key = K
    let codingPath: [CodingKey]
    let database: Blackbird.Database
    var row: Blackbird.Row
    
    init(codingPath: [CodingKey] = [], database: Blackbird.Database, row: Blackbird.Row) {
        self.database = database
        self.row = row
        self.codingPath = codingPath
    }
    
    var allKeys: [K] { row.keys.compactMap { K(stringValue: $0) } }
    func contains(_ key: K) -> Bool { row[key.stringValue] != nil }
    
    func decodeNil(forKey key: K) throws -> Bool {
        if let value = row[key.stringValue] { return value == .null }
        return true
    }
    
    func decode(_ type: Bool.Type, forKey key: K) throws -> Bool      { row[key.stringValue]?.boolValue ?? false }
    func decode(_ type: String.Type, forKey key: K) throws -> String  { row[key.stringValue]?.stringValue ?? "" }
    func decode(_ type: Double.Type, forKey key: K) throws -> Double  { row[key.stringValue]?.doubleValue ?? 0 }
    func decode(_ type: Float.Type, forKey key: K) throws -> Float    { Float(row[key.stringValue]?.doubleValue ?? 0) }
    func decode(_ type: Int.Type, forKey key: K) throws -> Int        { row[key.stringValue]?.intValue ?? 0 }
    func decode(_ type: Int8.Type, forKey key: K) throws -> Int8      { Int8(row[key.stringValue]?.intValue ?? 0) }
    func decode(_ type: Int16.Type, forKey key: K) throws -> Int16    { Int16(row[key.stringValue]?.intValue ?? 0) }
    func decode(_ type: Int32.Type, forKey key: K) throws -> Int32    { Int32(row[key.stringValue]?.intValue ?? 0) }
    func decode(_ type: Int64.Type, forKey key: K) throws -> Int64    { row[key.stringValue]?.int64Value ?? 0 }
    func decode(_ type: UInt.Type, forKey key: K) throws -> UInt      { UInt(row[key.stringValue]?.int64Value ?? 0) }
    func decode(_ type: UInt8.Type, forKey key: K) throws -> UInt8    { UInt8(row[key.stringValue]?.intValue ?? 0) }
    func decode(_ type: UInt16.Type, forKey key: K) throws -> UInt16  { UInt16(row[key.stringValue]?.intValue ?? 0) }
    func decode(_ type: UInt32.Type, forKey key: K) throws -> UInt32  { UInt32(row[key.stringValue]?.int64Value ?? 0) }
    func decode(_ type: UInt64.Type, forKey key: K) throws -> UInt64  { UInt64(row[key.stringValue]?.int64Value ?? 0) }
    func decode(_: Data.Type, forKey key: K) throws -> Data           { row[key.stringValue]?.dataValue ?? Data() }

    func decode(_: Date.Type, forKey key: K) throws -> Date {
        let timeInterval = try decode(Double.self, forKey: key)
        return Date(timeIntervalSince1970: timeInterval)
    }

    func decode(_: URL.Type, forKey key: K) throws -> URL {
        let string = try decode(String.self, forKey: key)
        guard let url = URL(string: string) else { throw BlackbirdSQLiteDecoder.Error.invalidValue(key.stringValue, value: string) }
        return url
    }

    func decode<T>(_ type: T.Type, forKey key: K) throws -> T where T: Decodable {
        if Data.self == T.self { return try decode(Data.self, forKey: key) as! T }
        if Date.self == T.self { return try decode(Date.self, forKey: key) as! T }
        if URL.self == T.self  { return try decode(URL.self,  forKey: key) as! T }
        
        var newPath = codingPath
        newPath.append(key)
        return try T(from: BlackbirdSQLiteDecoder(database: database, row: row, codingPath: newPath))
    }
    
    func nestedContainer<NestedKey>(keyedBy type: NestedKey.Type, forKey key: K) throws -> KeyedDecodingContainer<NestedKey> where NestedKey: CodingKey { fatalError("unsupported") }
    func nestedUnkeyedContainer(forKey key: K) throws -> UnkeyedDecodingContainer { fatalError("unsupported") }
    func superDecoder() throws -> Decoder { fatalError("unsupported") }
    func superDecoder(forKey key: K) throws -> Decoder { fatalError("unsupported") }
}
