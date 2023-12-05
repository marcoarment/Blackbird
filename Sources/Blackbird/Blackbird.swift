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
//  Blackbird.swift
//  Created by Marco Arment on 11/6/22.
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
import SQLite3

/// A small, fast, lightweight SQLite database wrapper and model layer.
public class Blackbird {
    /// A dictionary of argument values for a database query, keyed by column names.
    public typealias Arguments = Dictionary<String, Blackbird.Value>
    
    /// A set of primary-key values, where each is an array of values (to support multi-column primary keys).
    public typealias PrimaryKeyValues = Set<[Blackbird.Value]>

    /// A set of column names.
    public typealias ColumnNames = Set<String>

    /// Basic info for a ``BlackbirdColumn`` as returned by ``BlackbirdModel/columnInfoFromKeyPaths(_:)``.
    public struct ColumnInfo {
        /// The column's name.
        public let name: String
        
        /// The column's type.
        public let type: any BlackbirdColumnWrappable.Type
    }

    public enum Error: Swift.Error {
        /// Throw this within a `cancellableTransaction` block to cancel and roll back the transaction. It will not propagate further up the call stack.
        case cancelTransaction
    }

    /// A wrapper for SQLite's column data types.
    public enum Value: Sendable, ExpressibleByStringLiteral, ExpressibleByFloatLiteral, ExpressibleByBooleanLiteral, ExpressibleByIntegerLiteral, Hashable, Comparable {
        public static func < (lhs: Blackbird.Value, rhs: Blackbird.Value) -> Bool {
            switch lhs {
                case .null:           return false
                case let .integer(i): return i < rhs.int64Value ?? 0
                case let .double(d):  return d < rhs.doubleValue ?? 0
                case let .text(s):    return s < rhs.stringValue ?? ""
                case let .data(b):    return b.count < rhs.dataValue?.count ?? 0
            }
        }
        
        case null
        case integer(Int64)
        case double(Double)
        case text(String)
        case data(Data)

        public enum Error: Swift.Error {
            case cannotConvertToValue
        }

        public func hash(into hasher: inout Hasher) {
            hasher.combine(sqliteLiteral())
        }
        
        public static func fromAny(_ value: Any?) throws -> Value {
            guard var value else { return .null }
            
            if let optional = value as? any OptionalProtocol {
                if let wrapped = optional.wrappedOptionalValue {
                    value = wrapped
                } else {
                    return .null
                }
            }
            
            switch value {
                case _ as NSNull: return .null
                case let v as Value: return v
                case let v as any StringProtocol: return .text(String(v))
                case let v as any BlackbirdStorableAsInteger: return .integer(v.unifiedRepresentation())
                case let v as any BlackbirdStorableAsDouble: return .double(v.unifiedRepresentation())
                case let v as any BlackbirdStorableAsText: return .text(v.unifiedRepresentation())
                case let v as any BlackbirdStorableAsData: return .data(v.unifiedRepresentation())
                case let v as any BlackbirdIntegerEnum: return .integer(v.rawValue.unifiedRepresentation())
                case let v as any BlackbirdStringEnum: return .text(v.rawValue.unifiedRepresentation())
                default: throw Error.cannotConvertToValue
            }
        }

        public init(stringLiteral value: String) { self = .text(value) }
        public init(floatLiteral value: Double)  { self = .double(value) }
        public init(integerLiteral value: Int64) { self = .integer(value) }
        public init(booleanLiteral value: Bool)  { self = .integer(value ? 1 : 0) }
        
        public func sqliteLiteral() -> String {
            switch self {
                case let .integer(i): return String(i)
                case let .double(d):  return String(d)
                case let .text(s):    return "'\(s.replacingOccurrences(of: "'", with: "''"))'"
                case let .data(b):    return "X'\(b.map { String(format: "%02hhX", $0) }.joined())'"
                case .null:           return "NULL"
            }
        }
        
        public static func fromSQLiteLiteral(_ literalString: String) -> Self? {
            if literalString == "NULL" { return .null }
            
            if literalString.hasPrefix("'"), literalString.hasSuffix("'") {
                let start = literalString.index(literalString.startIndex, offsetBy: 1)
                let end = literalString.index(literalString.endIndex, offsetBy: -1)
                return .text(literalString[start..<end].replacingOccurrences(of: "''", with: "'"))
            }

            if literalString.hasPrefix("X'"), literalString.hasSuffix("'") {
                let start = literalString.index(literalString.startIndex, offsetBy: 2)
                let end = literalString.index(literalString.endIndex, offsetBy: -1)
                let hex = literalString[start..<end].replacingOccurrences(of: "''", with: "'")
                
                let hexChars = hex.map { $0 }
                let hexPairs = stride(from: 0, to: hexChars.count, by: 2).map { String(hexChars[$0]) + String(hexChars[$0 + 1]) }
                let bytes = hexPairs.compactMap { UInt8($0, radix: 16) }
                return .data(Data(bytes))
            }
            
            if let i = Int64(literalString) { return .integer(i) }
            if let d = Double(literalString) { return .double(d) }
            return nil
        }

        public var boolValue: Bool? {
            switch self {
                case .null:           return nil
                case let .integer(i): return i > 0
                case let .double(d):  return d > 0
                case let .text(s):    return (Int(s) ?? 0) != 0
                case let .data(b):    if let str = String(data: b, encoding: .utf8), let i = Int(str) { return i != 0 } else { return nil }
            }
        }

        public var dataValue: Data? {
            switch self {
                case .null:           return nil
                case let .data(b):    return b
                case let .integer(i): return String(i).data(using: .utf8)
                case let .double(d):  return String(d).data(using: .utf8)
                case let .text(s):    return s.data(using: .utf8)
            }
        }

        public var doubleValue: Double? {
            switch self {
                case .null:           return nil
                case let .double(d):  return d
                case let .integer(i): return Double(i)
                case let .text(s):    return Double(s)
                case let .data(b):    if let str = String(data: b, encoding: .utf8) { return Double(str) } else { return nil }
            }
        }

        public var intValue: Int? {
            switch self {
                case .null:           return nil
                case let .integer(i): return Int(i)
                case let .double(d):  return Int(d)
                case let .text(s):    return Int(s)
                case let .data(b):    if let str = String(data: b, encoding: .utf8) { return Int(str) } else { return nil }
            }
        }

        public var int64Value: Int64? {
            switch self {
                case .null:           return nil
                case let .integer(i): return Int64(i)
                case let .double(d):  return Int64(d)
                case let .text(s):    return Int64(s)
                case let .data(b):    if let str = String(data: b, encoding: .utf8) { return Int64(str) } else { return nil }
            }
        }

        public var stringValue: String? {
            switch self {
                case .null:           return nil
                case let .text(s):    return s
                case let .integer(i): return String(i)
                case let .double(d):  return String(d)
                case let .data(b):    return String(data: b, encoding: .utf8)
            }
        }

        internal func objcValue() -> NSObject {
            switch self {
                case .null:           return NSNull()
                case let .integer(i): return NSNumber(value: i)
                case let .double(d):  return NSNumber(value: d)
                case let .text(s):    return NSString(string: s)
                case let .data(d):    return NSData(data: d)
            }
        }

        private static let copyValue = unsafeBitCast(-1, to: sqlite3_destructor_type.self) // a.k.a. SQLITE_TRANSIENT
        
        internal func bind(database: isolated Blackbird.Database.Core, statement: OpaquePointer, index: Int32, for query: String) throws {
            var result: Int32
            switch self {
                case     .null:       result = sqlite3_bind_null(statement, index)
                case let .integer(i): result = sqlite3_bind_int64(statement, index, i)
                case let .double(d):  result = sqlite3_bind_double(statement, index, d)
                case let .text(s):    result = sqlite3_bind_text(statement, index, s, -1, Blackbird.Value.copyValue)
                case let .data(d):    result = d.withUnsafeBytes { bytes in sqlite3_bind_blob(statement, index, bytes.baseAddress, Int32(bytes.count), Blackbird.Value.copyValue) }
            }
            if result != SQLITE_OK { throw Blackbird.Database.Error.queryArgumentValueError(query: query, description: database.errorDesc(database.dbHandle)) }
        }
        
        internal func bind(database: isolated Blackbird.Database.Core, statement: OpaquePointer, name: String, for query: String) throws {
            let idx = sqlite3_bind_parameter_index(statement, name)
            if idx == 0 { throw Blackbird.Database.Error.queryArgumentNameError(query: query, name: name) }
            return try bind(database: database, statement: statement, index: idx, for: query)
        }
    }
}

// MARK: - Utilities

/// A basic locking utility.
public protocol BlackbirdLock: Sendable {
    func lock()
    func unlock()
    @discardableResult func withLock<R>(_ body: () throws -> R) rethrows -> R where R: Sendable
}
extension BlackbirdLock {
    @discardableResult public func withLock<R>(_ body: () throws -> R) rethrows -> R where R: Sendable {
        lock()
        defer { unlock() }
        return try body()
    }
}

import os
extension Blackbird {
    /// Blackbird's lock primitive, offered for public use.
    public static func Lock() -> BlackbirdLock {
        if #available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *) {
            return UnfairLock()
        } else {
            return LegacyUnfairLock()
        }
    }

    @available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
    fileprivate final class UnfairLock: BlackbirdLock {
        private let _lock = OSAllocatedUnfairLock()
        internal func lock() { _lock.lock() }
        internal func unlock() { _lock.unlock() }
    }

    fileprivate final class LegacyUnfairLock: BlackbirdLock, @unchecked Sendable /* unchecked due to known-safe use of an UnsafeMutablePointer */ {
        private var _lock: UnsafeMutablePointer<os_unfair_lock>
        internal func lock()   { os_unfair_lock_lock(_lock) }
        internal func unlock() { os_unfair_lock_unlock(_lock) }

        internal init() {
            _lock = UnsafeMutablePointer<os_unfair_lock>.allocate(capacity: 1)
            _lock.initialize(to: os_unfair_lock())
        }
        deinit { _lock.deallocate() }
    }

    /// Blackbird's locked-value utility, offered for public use. Useful when conforming to `Sendable`.
    public final class Locked<T: Sendable>: @unchecked Sendable /* unchecked due to use of internal locking */ {
        public var value: T {
            get {
                return lock.withLock { _value }
            }
            set {
                lock.withLock { _value = newValue }
            }
        }
        
        private let lock = Lock()
        private var _value: T
        
        public init(_ initialValue: T) {
            _value = initialValue
        }
        
        @discardableResult
        public func withLock<R>(_ body: (inout T) -> R) -> R where R: Sendable {
            return lock.withLock { return body(&_value) }
        }
    }

    internal final class FileChangeMonitor: @unchecked Sendable /* unchecked due to use of internal locking */ {
        private var sources: [DispatchSourceFileSystemObject] = []

        private var changeHandler: (() -> Void)?
        private var isClosed = false
        private var currentExpectedChanges = Set<Int64>()
        
        private let lock = Lock()

        public func addFile(filePath: String) {
            let fsPath = (filePath as NSString).fileSystemRepresentation
            let fd = open(fsPath, O_EVTONLY)
            guard fd >= 0 else { return }
            
            let source = DispatchSource.makeFileSystemObjectSource(fileDescriptor: fd, eventMask: [.write, .extend, .delete, .rename, .revoke], queue: nil)
            source.setCancelHandler { Darwin.close(fd) }
            
            source.setEventHandler { [weak self] in
                guard let self else { return }
                self.lock.lock()
                if self.currentExpectedChanges.isEmpty, !self.isClosed, let handler = self.changeHandler { handler() }
                self.lock.unlock()
            }

            source.activate()
            
            self.lock.lock()
            self.sources.append(source)
            self.lock.unlock()
        }
        
        deinit {
            cancel()
        }
        
        public func onChange(_ handler: @escaping (() -> Void)) {
            self.lock.lock()
            self.changeHandler = handler
            self.lock.unlock()
        }
        
        public func cancel() {
            self.lock.lock()
            self.isClosed = true
            for source in sources { source.cancel() }
            self.lock.unlock()
            
        }
        
        public func beginExpectedChange(_ changeID: Int64) {
            self.lock.lock()
            self.currentExpectedChanges.insert(changeID)
            self.lock.unlock()
        }
        
        public func endExpectedChange(_ changeID: Int64) {
            self.lock.lock()
            self.currentExpectedChanges.remove(changeID)
            self.lock.unlock()
        }
    }
    
    
    /// Blackbird's async-sempahore utility, offered for public use.
    ///
    /// Suggested use:
    ///
    /// ```swift
    /// class MyClass {
    ///     let myMethodSemaphore = Blackbird.Semaphore(value: 1)
    ///
    ///     func myMethod() async {
    ///         await myMethodSemaphore.wait()
    ///         defer { myMethodSemaphore.signal() }
    ///
    ///         // do async work...
    ///     }
    /// }
    /// ```
    /// Inspired by [Sebastian Toivonen's approach](https://forums.swift.org/t/semaphore-alternatives-for-structured-concurrency/59353/3).
    /// Consider using the Gwendal Roué's more-featured [Semaphore](https://github.com/groue/Semaphore) instead.
    public final class Semaphore: Sendable {
        private struct State: Sendable {
            var value = 0
            var waiting: [CheckedContinuation<Void, Never>] = []
        }
        private let state: Locked<State>

        public init(value: Int = 0) { state = Locked(State(value: value)) }
        
        public func wait() async {
            let wait = state.withLock { state in
                state.value -= 1
                return state.value < 0
            }
            
            if wait {
                await withCheckedContinuation { continuation in
                    state.withLock { $0.waiting.append(continuation) }
                }
            }
        }

        public func signal() {
            state.withLock { state in
                state.value += 1
                if state.waiting.isEmpty { return }
                state.waiting.removeFirst().resume()
            }
        }
    }
}
