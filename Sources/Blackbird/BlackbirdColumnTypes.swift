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
//  BlackbirdColumnTypes.swift
//  Created by Marco Arment on 1/14/23.
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

/// A wrapped data type supported by ``BlackbirdColumn``.
public protocol BlackbirdColumnWrappable: Hashable, Codable, Sendable {
    static func fromValue(_ value: Blackbird.Value) -> Self?
}

// MARK: - Column storage-type protocols

/// Internally represents data types compatible with SQLite's `INTEGER` type.
///
/// `UInt` and `UInt64` are intentionally omitted since SQLite integers max out at 64-bit signed.
public protocol BlackbirdStorableAsInteger: Codable {
    func unifiedRepresentation() -> Int64
    static func from(unifiedRepresentation: Int64) -> Self
}

/// Internally represents data types compatible with SQLite's `DOUBLE` type.
public protocol BlackbirdStorableAsDouble: Codable {
    func unifiedRepresentation() -> Double
    static func from(unifiedRepresentation: Double) -> Self
}

/// Internally represents data types compatible with SQLite's `TEXT` type.
public protocol BlackbirdStorableAsText: Codable {
    func unifiedRepresentation() -> String
    static func from(unifiedRepresentation: String) -> Self
}

/// Internally represents data types compatible with SQLite's `BLOB` type.
public protocol BlackbirdStorableAsData: Codable {
    func unifiedRepresentation() -> Data
    static func from(unifiedRepresentation: Data) -> Self
}

extension Double: BlackbirdColumnWrappable, BlackbirdStorableAsDouble {
    public func unifiedRepresentation() -> Double { self }
    public static func from(unifiedRepresentation: Double) -> Self { unifiedRepresentation }
    public static func fromValue(_ value: Blackbird.Value) -> Self? { value.doubleValue }
}

extension Float: BlackbirdColumnWrappable, BlackbirdStorableAsDouble {
    public func unifiedRepresentation() -> Double { Double(self) }
    public static func from(unifiedRepresentation: Double) -> Self { Float(unifiedRepresentation) }
    public static func fromValue(_ value: Blackbird.Value) -> Self? { if let d = value.doubleValue { return Float(d) } else { return nil } }
}

extension Date: BlackbirdColumnWrappable, BlackbirdStorableAsDouble {
    public func unifiedRepresentation() -> Double { self.timeIntervalSince1970 }
    public static func from(unifiedRepresentation: Double) -> Self { Date(timeIntervalSince1970: unifiedRepresentation) }
    public static func fromValue(_ value: Blackbird.Value) -> Self? { if let d = value.doubleValue { return Date(timeIntervalSince1970: d) } else { return nil } }
}

extension Data: BlackbirdColumnWrappable, BlackbirdStorableAsData {
    public func unifiedRepresentation() -> Data { self }
    public static func from(unifiedRepresentation: Data) -> Self { unifiedRepresentation }
    public static func fromValue(_ value: Blackbird.Value) -> Self? { value.dataValue }
}

extension String: BlackbirdColumnWrappable, BlackbirdStorableAsText {
    public func unifiedRepresentation() -> String { self }
    public static func from(unifiedRepresentation: String) -> Self { unifiedRepresentation }
    public static func fromValue(_ value: Blackbird.Value) -> Self? { value.stringValue }
}

extension URL: BlackbirdColumnWrappable, BlackbirdStorableAsText {
    public func unifiedRepresentation() -> String { self.absoluteString }
    public static func from(unifiedRepresentation: String) -> Self { URL(string: unifiedRepresentation)! }
    public static func fromValue(_ value: Blackbird.Value) -> Self? { if let s = value.stringValue { return URL(string: s) } else { return nil } }
}

extension Bool: BlackbirdColumnWrappable, BlackbirdStorableAsInteger {
    public func unifiedRepresentation() -> Int64 { Int64(self ? 1 : 0) }
    public static func from(unifiedRepresentation: Int64) -> Self { unifiedRepresentation == 0 ? false : true }
    public static func fromValue(_ value: Blackbird.Value) -> Self? { value.boolValue }
}

extension Int: BlackbirdColumnWrappable, BlackbirdStorableAsInteger {
    public func unifiedRepresentation() -> Int64 { Int64(self) }
    public static func from(unifiedRepresentation: Int64) -> Self { Int(unifiedRepresentation) }
    public static func fromValue(_ value: Blackbird.Value) -> Self? { value.intValue }
}

extension Int8: BlackbirdColumnWrappable, BlackbirdStorableAsInteger {
    public func unifiedRepresentation() -> Int64 { Int64(self) }
    public static func from(unifiedRepresentation: Int64) -> Self { Int8(unifiedRepresentation) }
    public static func fromValue(_ value: Blackbird.Value) -> Self? { if let i = value.intValue { return Int8(i) } else { return nil } }
}

extension Int16: BlackbirdColumnWrappable, BlackbirdStorableAsInteger {
    public func unifiedRepresentation() -> Int64 { Int64(self) }
    public static func from(unifiedRepresentation: Int64) -> Self { Int16(unifiedRepresentation) }
    public static func fromValue(_ value: Blackbird.Value) -> Self? { if let i = value.intValue { return Int16(i) } else { return nil } }
}

extension Int32: BlackbirdColumnWrappable, BlackbirdStorableAsInteger {
    public func unifiedRepresentation() -> Int64 { Int64(self) }
    public static func from(unifiedRepresentation: Int64) -> Self { Int32(unifiedRepresentation) }
    public static func fromValue(_ value: Blackbird.Value) -> Self? { if let i = value.intValue { return Int32(i) } else { return nil } }
}

extension Int64: BlackbirdColumnWrappable, BlackbirdStorableAsInteger {
    public func unifiedRepresentation() -> Int64 { self }
    public static func from(unifiedRepresentation: Int64) -> Self { unifiedRepresentation }
    public static func fromValue(_ value: Blackbird.Value) -> Self? { if let i = value.int64Value { return Int64(i) } else { return nil } }
}

extension UInt8: BlackbirdColumnWrappable, BlackbirdStorableAsInteger {
    public func unifiedRepresentation() -> Int64 { Int64(self) }
    public static func from(unifiedRepresentation: Int64) -> Self { UInt8(unifiedRepresentation) }
    public static func fromValue(_ value: Blackbird.Value) -> Self? { if let i = value.intValue { return UInt8(i) } else { return nil } }
}

extension UInt16: BlackbirdColumnWrappable, BlackbirdStorableAsInteger {
    public func unifiedRepresentation() -> Int64 { Int64(self) }
    public static func from(unifiedRepresentation: Int64) -> Self { UInt16(unifiedRepresentation) }
    public static func fromValue(_ value: Blackbird.Value) -> Self? { if let i = value.intValue { return UInt16(i) } else { return nil } }
}

extension UInt32: BlackbirdColumnWrappable, BlackbirdStorableAsInteger {
    public func unifiedRepresentation() -> Int64 { Int64(self) }
    public static func from(unifiedRepresentation: Int64) -> Self { UInt32(unifiedRepresentation) }
    public static func fromValue(_ value: Blackbird.Value) -> Self? { if let i = value.int64Value { return UInt32(i) } else { return nil } }
}

// MARK: - Enums, hacks for optionals

/// Declares an enum as compatible with Blackbird column storage, with a raw type of `String` or `URL`.
public protocol BlackbirdStringEnum: RawRepresentable, CaseIterable, BlackbirdColumnWrappable where RawValue: BlackbirdStorableAsText {
    associatedtype RawValue
}

/// Declares an enum as compatible with Blackbird column storage, with a Blackbird-compatible raw integer type such as `Int`.
public protocol BlackbirdIntegerEnum: RawRepresentable, CaseIterable, BlackbirdColumnWrappable where RawValue: BlackbirdStorableAsInteger {
    associatedtype RawValue
    static func unifiedRawValue(from unifiedRepresentation: Int64) -> RawValue
}

extension BlackbirdStringEnum {
    public static func fromValue(_ value: Blackbird.Value) -> Self? { if let s = value.stringValue { return Self(rawValue: RawValue.from(unifiedRepresentation: s)) } else { return nil } }
    
    internal static func defaultPlaceholderValue() -> Self { allCases.first! }
}

extension BlackbirdIntegerEnum {
    public static func unifiedRawValue(from unifiedRepresentation: Int64) -> RawValue { RawValue.from(unifiedRepresentation: unifiedRepresentation) }
    public static func fromValue(_ value: Blackbird.Value) -> Self? { if let i = value.int64Value { return Self(rawValue: Self.unifiedRawValue(from: i)) } else { return nil } }
    internal static func defaultPlaceholderValue() -> Self { allCases.first! }
}

extension Optional: BlackbirdColumnWrappable where Wrapped: BlackbirdColumnWrappable {
    public static func fromValue(_ value: Blackbird.Value) -> Self? { return Wrapped.fromValue(value) }
}

// Bad hack to make Optional<BlackbirdIntegerEnum> conform to BlackbirdStorableAsInteger
extension Optional: @retroactive RawRepresentable where Wrapped: RawRepresentable {
    public typealias RawValue = Wrapped.RawValue
    public init?(rawValue: Wrapped.RawValue) {
        if let w = Wrapped(rawValue: rawValue) { self = .some(w) } else { self = .none }
    }
    public var rawValue: Wrapped.RawValue { fatalError() }
}

extension Optional: @retroactive CaseIterable where Wrapped: CaseIterable {
    public static var allCases: [Optional<Wrapped>] { Wrapped.allCases.map { Optional<Wrapped>($0) } }
}

internal protocol BlackbirdIntegerOptionalEnum {
    static func nilInstance() -> Self
}

extension Optional: BlackbirdIntegerEnum, BlackbirdIntegerOptionalEnum where Wrapped: BlackbirdIntegerEnum {
    static func nilInstance() -> Self { .none }
}

internal protocol BlackbirdStringOptionalEnum {
    static func nilInstance() -> Self
}

extension Optional: BlackbirdStringEnum, BlackbirdStringOptionalEnum where Wrapped: BlackbirdStringEnum {
    static func nilInstance() -> Self { .none }
}
