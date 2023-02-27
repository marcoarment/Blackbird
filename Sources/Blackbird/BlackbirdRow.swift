//
//  BlackbirdRow.swift
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

// MARK: - Standard row

extension Blackbird {
    /// A dictionary of a single table row's values, keyed by their column names.
    public typealias Row = Dictionary<String, Blackbird.Value>
}

extension Blackbird.Row {
    public subscript<T: BlackbirdModel, V: BlackbirdColumnWrappable>(_ keyPath: KeyPath<T, BlackbirdColumn<Optional<V>>>) -> V? {
        let table = SchemaGenerator.shared.table(for: T.self)
        let columnName = table.keyPathToColumnName(keyPath: keyPath)

        guard let value = self[columnName], value != .null else { return nil }
        guard let typedValue = V.fromValue(value) else { fatalError("\(String(describing: T.self)).\(columnName) value in Blackbird.Row dictionary not convertible to \(String(describing: V.self))") }
        return typedValue
    }

    public subscript<T: BlackbirdModel, V: BlackbirdColumnWrappable>(_ keyPath: KeyPath<T, BlackbirdColumn<V>>) -> V {
        let table = SchemaGenerator.shared.table(for: T.self)
        let columnName = table.keyPathToColumnName(keyPath: keyPath)

        guard let value = self[columnName] else { fatalError("\(String(describing: T.self)).\(columnName) value not present in Blackbird.Row dictionary") }
        guard let typedValue = V.fromValue(value) else { fatalError("\(String(describing: T.self)).\(columnName) value in Blackbird.Row dictionary not convertible to \(String(describing: V.self))") }
        return typedValue
    }
}


// MARK: - Model-specific row
// This allows typed key-pair lookups without specifying the type name at the call site, e.g.:
//
//   row[\.$title]
//
//     instead of
//
//   row[\MyModelName.$title]
//
extension Blackbird {
    /// A specialized version of ``Row`` associated with its source ``BlackbirdModel`` type for convenient access to its values with column key-paths.
    public struct ModelRow<T: BlackbirdModel>: Collection, Equatable {
        internal init(_ row: Blackbird.Row) { dictionary = row }
        public var row: Blackbird.Row {
            get { dictionary }
        }
    
        public subscript<V: BlackbirdColumnWrappable>(_ keyPath: KeyPath<T, BlackbirdColumn<Optional<V>>>) -> V? {
            let table = SchemaGenerator.shared.table(for: T.self)
            let columnName = table.keyPathToColumnName(keyPath: keyPath)
            
            guard let value = dictionary[columnName], value != .null else { return nil }
            guard let typedValue = V.fromValue(value) else { fatalError("\(String(describing: T.self)).\(columnName) value in Blackbird.Row dictionary not convertible to \(String(describing: V.self))") }
            return typedValue
        }

        public subscript<V: BlackbirdColumnWrappable>(_ keyPath: KeyPath<T, BlackbirdColumn<V>>) -> V {
            let table = SchemaGenerator.shared.table(for: T.self)
            let columnName = table.keyPathToColumnName(keyPath: keyPath)
            
            guard let value = dictionary[columnName] else { fatalError("\(String(describing: T.self)).\(columnName) value not present in Blackbird.Row dictionary") }
            guard let typedValue = V.fromValue(value) else { fatalError("\(String(describing: T.self)).\(columnName) value in Blackbird.Row dictionary not convertible to \(String(describing: V.self))") }
            return typedValue
        }


        // Collection conformance
        public typealias DictionaryType = Dictionary<String, Blackbird.Value>
        public typealias Index = DictionaryType.Index
        private var dictionary: DictionaryType = [:]
        public var keys: Dictionary<String, Blackbird.Value>.Keys { dictionary.keys }
        public typealias Indices = DictionaryType.Indices
        public typealias Iterator = DictionaryType.Iterator
        public typealias SubSequence = DictionaryType.SubSequence
        public var startIndex: Index { dictionary.startIndex }
        public var endIndex: DictionaryType.Index { dictionary.endIndex }
        public subscript(position: Index) -> Iterator.Element { dictionary[position] }
        public subscript(bounds: Range<Index>) -> SubSequence { dictionary[bounds] }
        public var indices: Indices { dictionary.indices }
        public subscript(key: String) -> Blackbird.Value? {
            get { dictionary[key] }
            set { dictionary[key] = newValue }
        }
        public func index(after i: Index) -> Index { dictionary.index(after: i) }
        public func makeIterator() -> DictionaryIterator<String, Blackbird.Value> { dictionary.makeIterator() }
    }
}

