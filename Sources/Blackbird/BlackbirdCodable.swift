//
//  BlackbirdCodable.swift
//  Created by Marco Arment on 11/7/22.
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

// with significant thanks to (and borrowing from) https://shareup.app/blog/encoding-and-decoding-sqlite-in-swift/

internal class BlackbirdSQLiteEncoder: Encoder {
    fileprivate class Storage {
        private var elements = Blackbird.Arguments()
        var arguments: Blackbird.Arguments { elements }

        func reset() { elements.removeAll(keepingCapacity: true) }

        subscript(key: String) -> Blackbird.Value? {
            get { elements[key] }
            set { elements[key] = newValue }
        }
    }

    public var codingPath: [CodingKey] = []
    private let storage = Storage()
    public var userInfo: [CodingUserInfoKey: Any] = [:]
    public var sqliteArguments: Blackbird.Arguments { storage.arguments }
    
    public func container<Key>(keyedBy type: Key.Type) -> KeyedEncodingContainer<Key> where Key : CodingKey {
        storage.reset()
        return KeyedEncodingContainer(BlackbirdSQLiteKeyedEncodingContainer<Key>(storage))
    }
    public func unkeyedContainer() -> UnkeyedEncodingContainer { fatalError("unsupported") }
    public func singleValueContainer() -> SingleValueEncodingContainer { BlackbirdSQLiteSingleValueEncodingContainer(storage) }
}

internal struct BlackbirdSQLiteSingleValueEncodingContainer: SingleValueEncodingContainer {
    var codingPath: [CodingKey] = []
    private var storage: BlackbirdSQLiteEncoder.Storage

    fileprivate init(_ storage: BlackbirdSQLiteEncoder.Storage) {
        self.storage = storage
    }

    public enum Error: Swift.Error {
        case invalidValue(String)
    }

    mutating public func encodeNil() throws             { storage[""] = .null }
    mutating public func encode(_ value: Bool) throws   { storage[""] = .integer(value ? 1 : 0) }
    mutating public func encode(_ value: Int) throws    { storage[""] = .integer(Int64(value)) }
    mutating public func encode(_ value: Int8) throws   { storage[""] = .integer(Int64(value)) }
    mutating public func encode(_ value: Int16) throws  { storage[""] = .integer(Int64(value)) }
    mutating public func encode(_ value: Int32) throws  { storage[""] = .integer(Int64(value)) }
    mutating public func encode(_ value: Int64) throws  { storage[""] = .integer(value) }
    mutating public func encode(_ value: UInt8) throws  { storage[""] = .integer(Int64(value)) }
    mutating public func encode(_ value: UInt16) throws { storage[""] = .integer(Int64(value)) }
    mutating public func encode(_ value: UInt32) throws { storage[""] = .integer(Int64(value)) }
    mutating public func encode(_ value: Float) throws  { storage[""] = .double(Double(value)) }
    mutating public func encode(_ value: Double) throws { storage[""] = .double(value) }
    mutating public func encode(_ value: String) throws { storage[""] = .text(value) }
    mutating public func encode(_ value: Data) throws   { storage[""] = .data(value) }
    mutating public func encode(_ value: Date) throws   { storage[""] = .double(value.timeIntervalSince1970) }
    mutating public func encode(_ value: URL) throws    { storage[""] = .text(value.absoluteString) }

    mutating public func encode(_ value: UInt) throws {
        guard value <= Int64.max else { throw Error.invalidValue(codingPath.first?.stringValue ?? "") }
        storage[""] = .integer(Int64(value))
    }

    mutating public func encode(_ value: UInt64) throws {
        guard value <= Int64.max else { throw Error.invalidValue(codingPath.first?.stringValue ?? "") }
        storage[""] = .integer(Int64(value))
    }
    
    mutating func encode<T>(_ value: T) throws where T : Encodable { fatalError("unsupported") }
}

internal struct BlackbirdSQLiteKeyedEncodingContainer<K: CodingKey>: KeyedEncodingContainerProtocol {
    public typealias Key = K
    public var codingPath: [CodingKey] = []
    private var storage: BlackbirdSQLiteEncoder.Storage

    public enum Error: Swift.Error {
        case invalidValue(String, value: String)
    }

    fileprivate init(_ storage: BlackbirdSQLiteEncoder.Storage) {
        self.storage = storage
    }
    
    mutating public func encodeNil(forKey key: K) throws               { storage[key.stringValue] = .null }
    mutating public func encode(_ value: Bool, forKey key: K) throws   { storage[key.stringValue] = .integer(value ? 1 : 0) }
    mutating public func encode(_ value: Int, forKey key: K) throws    { storage[key.stringValue] = .integer(Int64(value)) }
    mutating public func encode(_ value: Int8, forKey key: K) throws   { storage[key.stringValue] = .integer(Int64(value)) }
    mutating public func encode(_ value: Int16, forKey key: K) throws  { storage[key.stringValue] = .integer(Int64(value)) }
    mutating public func encode(_ value: Int32, forKey key: K) throws  { storage[key.stringValue] = .integer(Int64(value)) }
    mutating public func encode(_ value: Int64, forKey key: K) throws  { storage[key.stringValue] = .integer(value) }
    mutating public func encode(_ value: UInt8, forKey key: K) throws  { storage[key.stringValue] = .integer(Int64(value)) }
    mutating public func encode(_ value: UInt16, forKey key: K) throws { storage[key.stringValue] = .integer(Int64(value)) }
    mutating public func encode(_ value: UInt32, forKey key: K) throws { storage[key.stringValue] = .integer(Int64(value)) }
    mutating public func encode(_ value: Float, forKey key: K) throws  { storage[key.stringValue] = .double(Double(value)) }
    mutating public func encode(_ value: Double, forKey key: K) throws { storage[key.stringValue] = .double(value) }
    mutating public func encode(_ value: String, forKey key: K) throws { storage[key.stringValue] = .text(value) }
    mutating public func encode(_ value: Data, forKey key: K) throws   { storage[key.stringValue] = .data(value) }
    mutating public func encode(_ value: Date, forKey key: K) throws   { storage[key.stringValue] = .double(value.timeIntervalSince1970) }
    mutating public func encode(_ value: URL, forKey key: K) throws    { storage[key.stringValue] = .text(value.absoluteString) }

    mutating public func encode(_ value: UInt, forKey key: K) throws {
        guard value <= Int64.max else { throw Error.invalidValue(key.stringValue, value: key.stringValue) }
        storage[key.stringValue] = .integer(Int64(value))
    }

    mutating public func encode(_ value: UInt64, forKey key: K) throws {
        guard value <= Int64.max else { throw Error.invalidValue(key.stringValue, value: key.stringValue) }
        storage[key.stringValue] = .integer(Int64(value))
    }

    mutating public func encode(_ value: some Encodable, forKey key: K) throws {
        if let data = value as? Data { try encode(data, forKey: key) }
        else if let date = value as? Date { try encode(date, forKey: key) }
        else if let url = value as? URL { try encode(url, forKey: key) }
        else if let columnWrapper = value as? any ColumnWrapper { try encode(columnWrapper.value, forKey: key) }
        else {
            let subencoder = BlackbirdSQLiteEncoder()
            try value.encode(to: subencoder)
            let encoded = subencoder.sqliteArguments
            if encoded.count == 1, let encodedValue = encoded.first?.value { storage[key.stringValue] = encodedValue }
            else { throw Error.invalidValue(key.stringValue, value: key.stringValue) }
        }
    }

    mutating public func nestedContainer<NestedKey>(keyedBy keyType: NestedKey.Type, forKey key: K) -> KeyedEncodingContainer<NestedKey> where NestedKey : CodingKey { fatalError("unsupported") }
    mutating public func nestedUnkeyedContainer(forKey key: K) -> UnkeyedEncodingContainer { fatalError("unsupported") }
    mutating public func superEncoder() -> Encoder { fatalError("unsupported") }
    mutating public func superEncoder(forKey key: K) -> Encoder { fatalError("unsupported") }
}

internal class BlackbirdSQLiteDecoder: Decoder {
    public enum Error: Swift.Error {
        case invalidValue(String, value: String)
        case missingValue(String)
    }

    public var codingPath: [CodingKey] = []
    public var userInfo: [CodingUserInfoKey: Any] = [:]

    let row: Blackbird.Row
    init(_ row: Blackbird.Row, codingPath: [CodingKey] = []) {
        self.row = row
        self.codingPath = codingPath
    }

    public func container<Key>(keyedBy type: Key.Type) throws -> KeyedDecodingContainer<Key> where Key : CodingKey {
        return KeyedDecodingContainer(BlackbirdSQLiteKeyedDecodingContainer<Key>(codingPath: codingPath, row: row))
    }
    public func unkeyedContainer() throws -> UnkeyedDecodingContainer { fatalError("unsupported") }
    public func singleValueContainer() throws -> SingleValueDecodingContainer { BlackbirdSQLiteSingleValueDecodingContainer(codingPath: codingPath, row: row) }
}

fileprivate struct BlackbirdSQLiteSingleValueDecodingContainer: SingleValueDecodingContainer {
    var codingPath: [CodingKey] = []
    var row: Blackbird.Row
    
    init(codingPath: [CodingKey], row: Blackbird.Row) {
        self.codingPath = codingPath
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
        if T.self == Data.self { return ((try value()).dataValue ?? Data()) as! T }
        if T.self == URL.self, let urlStr = (try value()).stringValue, let url = URL(string: urlStr) { return url as! T }
        return try T(from: BlackbirdSQLiteDecoder(row, codingPath: codingPath))
    }
}

fileprivate class BlackbirdSQLiteKeyedDecodingContainer<K: CodingKey>: KeyedDecodingContainerProtocol {
    typealias Key = K
    let codingPath: [CodingKey]
    var row: Blackbird.Row
    
    init(codingPath: [CodingKey] = [], row: Blackbird.Row) {
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

    func decode<T>(_ type: T.Type, forKey key: K) throws -> T where T : Decodable {
        if Data.self == T.self { return try decode(Data.self, forKey: key) as! T }
        if Date.self == T.self { return try decode(Date.self, forKey: key) as! T }
        if URL.self == T.self  { return try decode(URL.self,  forKey: key) as! T }
        
        var newPath = codingPath
        newPath.append(key)
        return try T(from: BlackbirdSQLiteDecoder(row, codingPath: newPath))
    }
    
    func nestedContainer<NestedKey>(keyedBy type: NestedKey.Type, forKey key: K) throws -> KeyedDecodingContainer<NestedKey> where NestedKey : CodingKey { fatalError("unsupported") }
    func nestedUnkeyedContainer(forKey key: K) throws -> UnkeyedDecodingContainer { fatalError("unsupported") }
    func superDecoder() throws -> Decoder { fatalError("unsupported") }
    func superDecoder(forKey key: K) throws -> Decoder { fatalError("unsupported") }
}
