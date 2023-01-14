//
//  File.swift
//  
//
//  Created by Marco Arment on 1/14/23.
//

import Foundation

/// A wrapped data type supported by ``BlackbirdColumn``.
public protocol BlackbirdColumnWrappable: Hashable, Codable, Sendable { }


// MARK: - Column storage-type protocols

// UInt, UInt64 intentionally omitted from BlackbirdStorableAsInteger since SQLite integers max out at 64-bit signed

public protocol BlackbirdStorableAsInteger: Codable {
    func unifiedRepresentation() -> Int64
    static func from(unifiedRepresentation: Int64) -> Self
}

public protocol BlackbirdStorableAsDouble: Codable {
    func unifiedRepresentation() -> Double
    static func from(unifiedRepresentation: Double) -> Self
}

public protocol BlackbirdStorableAsText: Codable {
    func unifiedRepresentation() -> String
    static func from(unifiedRepresentation: String) -> Self
}

public protocol BlackbirdStorableAsData: Codable {
    func unifiedRepresentation() -> Data
    static func from(unifiedRepresentation: Data) -> Self
}

extension Double: BlackbirdColumnWrappable, BlackbirdStorableAsDouble {
    public func unifiedRepresentation() -> Double { self }
    public static func from(unifiedRepresentation: Double) -> Self { unifiedRepresentation }
}

extension Float: BlackbirdColumnWrappable, BlackbirdStorableAsDouble {
    public func unifiedRepresentation() -> Double { Double(self) }
    public static func from(unifiedRepresentation: Double) -> Self { Float(unifiedRepresentation) }
}

extension Date: BlackbirdColumnWrappable, BlackbirdStorableAsDouble {
    public func unifiedRepresentation() -> Double { self.timeIntervalSince1970 }
    public static func from(unifiedRepresentation: Double) -> Self { Date(timeIntervalSince1970: unifiedRepresentation) }
}

extension Data: BlackbirdColumnWrappable, BlackbirdStorableAsData {
    public func unifiedRepresentation() -> Data { self }
    public static func from(unifiedRepresentation: Data) -> Self { unifiedRepresentation }
}

extension String: BlackbirdColumnWrappable, BlackbirdStorableAsText {
    public func unifiedRepresentation() -> String { self }
    public static func from(unifiedRepresentation: String) -> Self { unifiedRepresentation }
}

extension URL: BlackbirdColumnWrappable, BlackbirdStorableAsText {
    public func unifiedRepresentation() -> String { self.absoluteString }
    public static func from(unifiedRepresentation: String) -> Self { URL(string: unifiedRepresentation)! }
}

extension Bool: BlackbirdColumnWrappable, BlackbirdStorableAsInteger {
    public func unifiedRepresentation() -> Int64 { Int64(self ? 1 : 0) }
    public static func from(unifiedRepresentation: Int64) -> Self { unifiedRepresentation == 0 ? false : true }
}

extension Int: BlackbirdColumnWrappable, BlackbirdStorableAsInteger {
    public func unifiedRepresentation() -> Int64 { Int64(self) }
    public static func from(unifiedRepresentation: Int64) -> Self { Int(unifiedRepresentation) }
}

extension Int8: BlackbirdColumnWrappable, BlackbirdStorableAsInteger {
    public func unifiedRepresentation() -> Int64 { Int64(self) }
    public static func from(unifiedRepresentation: Int64) -> Self { Int8(unifiedRepresentation) }
}

extension Int16: BlackbirdColumnWrappable, BlackbirdStorableAsInteger {
    public func unifiedRepresentation() -> Int64 { Int64(self) }
    public static func from(unifiedRepresentation: Int64) -> Self { Int16(unifiedRepresentation) }
}

extension Int32: BlackbirdColumnWrappable, BlackbirdStorableAsInteger {
    public func unifiedRepresentation() -> Int64 { Int64(self) }
    public static func from(unifiedRepresentation: Int64) -> Self { Int32(unifiedRepresentation) }
}

extension Int64: BlackbirdColumnWrappable, BlackbirdStorableAsInteger {
    public func unifiedRepresentation() -> Int64 { self }
    public static func from(unifiedRepresentation: Int64) -> Self { unifiedRepresentation }
}

extension UInt8: BlackbirdColumnWrappable, BlackbirdStorableAsInteger {
    public func unifiedRepresentation() -> Int64 { Int64(self) }
    public static func from(unifiedRepresentation: Int64) -> Self { UInt8(unifiedRepresentation) }
}

extension UInt16: BlackbirdColumnWrappable, BlackbirdStorableAsInteger {
    public func unifiedRepresentation() -> Int64 { Int64(self) }
    public static func from(unifiedRepresentation: Int64) -> Self { UInt16(unifiedRepresentation) }
}

extension UInt32: BlackbirdColumnWrappable, BlackbirdStorableAsInteger {
    public func unifiedRepresentation() -> Int64 { Int64(self) }
    public static func from(unifiedRepresentation: Int64) -> Self { UInt32(unifiedRepresentation) }
}

// MARK: - Enums, hacks for optionals

/// Declares an enum as compatible with Blackbird column storage, with a raw type of `String` or `URL`.
public protocol BlackbirdStringEnum: RawRepresentable, BlackbirdColumnWrappable where RawValue: BlackbirdStorableAsText { }

/// Declares an enum as compatible with Blackbird column storage, with a Blackbird-compatible raw integer type such as `Int`.
public protocol BlackbirdIntegerEnum: RawRepresentable, BlackbirdColumnWrappable where RawValue: BlackbirdStorableAsInteger {
    associatedtype RawValue
    static func unifiedRawValue(from unifiedRepresentation: Int64) -> RawValue
}

extension BlackbirdIntegerEnum {
    public static func unifiedRawValue(from unifiedRepresentation: Int64) -> RawValue { RawValue.from(unifiedRepresentation: unifiedRepresentation) }
}

extension Optional: BlackbirdColumnWrappable where Wrapped: BlackbirdColumnWrappable { }

// Bad hack to make Optional<BlackbirdIntegerEnum> conform to BlackbirdStorableAsInteger
extension Optional: RawRepresentable where Wrapped: RawRepresentable {
    public typealias RawValue = Wrapped.RawValue
    public init?(rawValue: Wrapped.RawValue) {
        if let w = Wrapped(rawValue: rawValue) { self = .some(w) } else { self = .none }
    }
    public var rawValue: Wrapped.RawValue { fatalError() }
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
