//
//  BlackbirdColumn.swift
//  Created by Marco Arment on 2/27/23.
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

import Foundation

internal protocol ColumnWrapper: WrappedType {
    associatedtype ValueType: BlackbirdColumnWrappable
    var value: ValueType { get }
    func hasChanged(in database: Blackbird.Database) -> Bool
    func clearHasChanged(in database: Blackbird.Database)
    var internalNameInSchemaGenerator: Blackbird.Locked<String?> { get }
}

@propertyWrapper public struct BlackbirdColumn<T>: ColumnWrapper, WrappedType, Equatable, Sendable, Codable where T: BlackbirdColumnWrappable {
    public static func == (lhs: Self, rhs: Self) -> Bool { type(of: lhs) == type(of: rhs) && lhs.value == rhs.value }
    
    private var _value: T
    internal final class ColumnState<T>: @unchecked Sendable /* unchecked due to external locking in all uses */ {
        var hasChanged: Bool
        weak var lastUsedDatabase: Blackbird.Database?
        
        init(hasChanged: Bool, lastUsedDatabase: Blackbird.Database? = nil) {
            self.hasChanged = hasChanged
            self.lastUsedDatabase = lastUsedDatabase
        }
    }
    
    private let state: Blackbird.Locked<ColumnState<T>>
    let internalNameInSchemaGenerator = Blackbird.Locked<String?>(nil)

    public var value: T {
        get { state.withLock { _ in self._value } }
        set { self.wrappedValue = newValue }
    }

    public var projectedValue: BlackbirdColumn<T> { self }
    static internal func schemaGeneratorWrappedType() -> Any.Type { T.self }

    public var wrappedValue: T {
        get { state.withLock { _ in self._value } }
        set {
            state.withLock { state in
                guard self._value != newValue else { return }
                self._value = newValue
                state.hasChanged = true
            }
        }
    }
    
    public func hasChanged(in database: Blackbird.Database) -> Bool {
        state.withLock { state in
            if state.lastUsedDatabase != database { return true }
            return state.hasChanged
        }
    }
    
    internal func clearHasChanged(in database: Blackbird.Database) {
        state.withLock { state in
            state.lastUsedDatabase = database
            state.hasChanged = false
        }
    }

    public init(wrappedValue: T) {
        _value = wrappedValue
        state = Blackbird.Locked(ColumnState(hasChanged: true, lastUsedDatabase: nil))
    }
    
    public init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        let value = try container.decode(T.self)
        _value = value
        if let sqliteDecoder = decoder as? BlackbirdSQLiteDecoder {
            state = Blackbird.Locked(ColumnState(hasChanged: false, lastUsedDatabase: sqliteDecoder.database))
        } else {
            state = Blackbird.Locked(ColumnState(hasChanged: true, lastUsedDatabase: nil))
        }
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        try container.encode(self.value)
    }
}

// MARK: - Accessing wrapped types of optionals and column wrappers
internal protocol OptionalProtocol {
    var wrappedOptionalValue: Any? { get }
}
extension Optional: OptionalProtocol {
    var wrappedOptionalValue: Any? {
        get {
            switch self {
                case .some(let w): return w
                default: return nil
            }
        }
    }
}

internal protocol WrappedType {
    static func schemaGeneratorWrappedType() -> Any.Type
}

extension Optional: WrappedType {
    internal static func schemaGeneratorWrappedType() -> Any.Type { Wrapped.self }
}

