//
//  BlackbirdSwiftUI.swift
//  Created by Marco Arment on 12/5/22.
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

import SwiftUI
import Combine

struct EnvironmentBlackbirdDatabaseKey: EnvironmentKey {
    static var defaultValue: Blackbird.Database? = nil
}

extension EnvironmentValues {
    /// The ``Blackbird/Database`` to use with `@BlackbirdLive…` property wrappers.
    public var blackbirdDatabase: Blackbird.Database? {
        get { self[EnvironmentBlackbirdDatabaseKey.self] }
        set { self[EnvironmentBlackbirdDatabaseKey.self] = newValue }
    }
}


// MARK: - Fetch property wrappers

/// An array of database rows produced by a generator function, kept up-to-date as data changes in the specified table.
///
/// Set `@Environment(\.blackbirdDatabase)` to the desired database instance to read.
///
/// The generator is passed the current database as its sole argument (`$0`).
///
/// Example:
///
/// ```swift
/// @BlackbirdLiveQuery(tableName: "Post", {
///     try await $0.query("SELECT COUNT(*) AS c FROM Post")
/// }) var countResults
///
/// // countResults.first["c"] will be the resulting Blackbird.Value
/// ```
@propertyWrapper public struct BlackbirdLiveQuery: DynamicProperty {
    @State private var results: [Blackbird.Row] = []
    @State private var didLoad = false
    @Environment(\.blackbirdDatabase) var environmentDatabase

    public var wrappedValue: [Blackbird.Row] {
        get { results }
        set { }
    }

    private let queryUpdater: Blackbird.QueryUpdater
    private let generator: Blackbird.CachedResultGenerator<[Blackbird.Row]>
    private let tableName: String

    public init(tableName: String, _ generator: @escaping Blackbird.CachedResultGenerator<[Blackbird.Row]>) {
        self.tableName = tableName
        self.generator = generator
        self.queryUpdater = Blackbird.QueryUpdater()
    }

    public func update() {
        queryUpdater.bind(from: environmentDatabase, tableName: tableName, to: $results, didLoad: $didLoad, generator: generator)
    }
}

/// An array of ``BlackbirdModel`` instances produced by a generator function, kept up-to-date as their table's data changes in the database.
///
/// Set `@Environment(\.blackbirdDatabase)` to the desired database instance to read.
///
/// The generator is passed the current database as its sole argument (`$0`).
///
/// Example:
///
/// ```swift
/// @BlackbirdLiveModels({
///     try await Post.read(from: $0, where: "id > 3 ORDER BY date")
/// }) var posts
///
/// // posts will be an array of Post models matching the query
/// ```
@propertyWrapper public struct BlackbirdLiveModels<T: BlackbirdModel>: DynamicProperty {
    @State private var results: [T] = []
    @Environment(\.blackbirdDatabase) var environmentDatabase
    
    public var wrappedValue: [T] {
        get { results }
        set { }
    }
    
    private let queryUpdater = Blackbird.ModelArrayUpdater<T>()
    private let generator: Blackbird.CachedResultGenerator<[T]>

    public init(_ generator: @escaping Blackbird.CachedResultGenerator<[T]>) {
        self.generator = generator
    }

    public func update() {
        queryUpdater.bind(from: environmentDatabase, to: $results, generator: generator)
    }
}

/// A single ``BlackbirdModel`` instance, kept up-to-date as its data changes in the database.
///
/// Set `@Environment(\.blackbirdDatabase)` to the desired database instance to read.
///
/// The ``BlackbirdModel/liveModel`` property is helpful when initializing child views with a specific instance.
///
/// Example:
///
/// ```swift
/// // In a parent view:
/// ForEach(posts) { post in
///     NavigationLink(destination: PostView(post: post.liveModel)) {
///         Text(post.title)
///     }
/// }
///
/// // Child view:
/// struct PostView: View {
///     @BlackbirdLiveModel var post: Post?
///     // will be kept up-to-date
/// }
/// ```
@propertyWrapper public struct BlackbirdLiveModel<T: BlackbirdModel>: DynamicProperty {
    @State private var instance: T?
    @Environment(\.blackbirdDatabase) var environmentDatabase
    
    public var wrappedValue: T? {
        get { instance }
        set { }
    }
    
    private let queryUpdater = Blackbird.ModelUpdater<T>()
    private var generator: Blackbird.CachedResultGenerator<T?>?

    public init(_ instance: T) {
        do {
            let encoder = BlackbirdSQLiteEncoder()
            try instance.encode(to: encoder)
            let encodedValues = encoder.sqliteArguments()
            let primaryKeyValues = T.table.primaryKeys.map { encodedValues[$0.name]! }
            generator = { try await T.read(from: $0, multicolumnPrimaryKey: primaryKeyValues) }
        } catch {
            print("[Blackbird.BlackbirdLiveModel<\(String(describing: T.self))>] ⚠️ Error getting primary key values: \(error.localizedDescription)")
        }

        _instance = State(initialValue: instance)
    }

    public func update() {
        queryUpdater.bind(from: environmentDatabase, to: $instance, generator: generator)
    }
}

extension BlackbirdModel {
    public var liveModel: BlackbirdLiveModel<Self> { get { BlackbirdLiveModel(self) } }
}

// MARK: - Multi-row query updaters

extension Blackbird {
    public class QueryUpdater {
        @Binding public var results: [Blackbird.Row]
        @Binding public var didLoad: Bool

        private var resultPublisher = CachedResultPublisher<[Blackbird.Row]>()
        private var changePublishers: [AnyCancellable] = []

        public init() {
            _results = Binding<[Blackbird.Row]>(get: { [] }, set: { _ in })
            _didLoad = Binding<Bool>(get: { false }, set: { _ in })
        }
        
        public func bind(from database: Blackbird.Database?, tableName: String, to results: Binding<[Blackbird.Row]>, didLoad: Binding<Bool>? = nil, generator: CachedResultGenerator<[Blackbird.Row]>?) {
            changePublishers.removeAll()
            resultPublisher.subscribe(to: tableName, in: database, generator: generator)
            _results = results
            if let didLoad { _didLoad = didLoad }
            
            changePublishers.append(resultPublisher.valuePublisher.sink { [weak self] value in
                guard let self else { return }
                DispatchQueue.main.async { // kicking this to the next runloop to prevent state updates from happening while building the view
                    if let value {
                        self.results = value
                        self.didLoad = true
                    } else {
                        self.results = []
                    }
                }
            })
        }
    }

    public class ModelArrayUpdater<T: BlackbirdModel> {
        @Binding public var results: [T]
        @Binding public var didLoad: Bool

        private var resultPublisher = CachedResultPublisher<[T]>()
        private var changePublishers: [AnyCancellable] = []

        public init() {
            _results = Binding<[T]>(get: { [] }, set: { _ in })
            _didLoad = Binding<Bool>(get: { false }, set: { _ in })
        }
        
        public func bind(from database: Blackbird.Database?, to results: Binding<[T]>, didLoad: Binding<Bool>? = nil, generator: CachedResultGenerator<[T]>?) {
            changePublishers.removeAll()
            resultPublisher.subscribe(to: T.table.name(type: T.self), in: database, generator: generator)
            _results = results
            if let didLoad { _didLoad = didLoad }
            
            changePublishers.append(resultPublisher.valuePublisher.sink { [weak self] value in
                guard let self else { return }
                DispatchQueue.main.async {
                    if let value {
                        self.results = value
                        self.didLoad = true
                    } else {
                        self.results = []
                    }
                }
            })
        }
    }

    public class ModelUpdater<T: BlackbirdModel> {
        @Binding public var instance: T?
        @Binding public var didLoad: Bool

        private var resultPublisher = CachedResultPublisher<T?>()
        private var changePublishers: [AnyCancellable] = []

        public init() {
            _instance = Binding<T?>(get: { nil }, set: { _ in })
            _didLoad = Binding<Bool>(get: { false }, set: { _ in })
        }
        
        public func bind(from database: Blackbird.Database?, to instance: Binding<T?>, didLoad: Binding<Bool>? = nil, generator: CachedResultGenerator<T?>?) {
            changePublishers.removeAll()
            resultPublisher.subscribe(to: T.table.name(type: T.self), in: database, generator: generator)
            _instance = instance
            if let didLoad { _didLoad = didLoad }
            
            changePublishers.append(resultPublisher.valuePublisher.sink { [weak self] value in
                guard let self else { return }
                DispatchQueue.main.async {
                    if let value {
                        self.instance = value
                        self.didLoad = true
                    } else {
                        self.instance = nil
                    }
                }
            })
        }
    }

}


// MARK: - Single-instance updater

extension Blackbird {
    public class ModelInstanceUpdater<T: BlackbirdModel> {
        @Binding public var instance: T?
        @Binding public var didLoad: Bool
        
        private var database: Blackbird.Database? = nil
        private var updater: ((_ db: Blackbird.Database) async throws -> T?)? = nil
        private var changePublisher: AnyCancellable? = nil
        private var watchedPrimaryKeys = Blackbird.PrimaryKeyValues()

        public init() {
            _instance = Binding<T?>(get: { nil }, set: { _ in })
            _didLoad = Binding<Bool>(get: { false }, set: { _ in })
        }
                
        /// Update a binding with the current instance matching a single-column primary-key value, and keep it updated over time.
        /// - Parameters:
        ///   - database: The database to read from and monitor for changes.
        ///   - instance: A binding to store the matching instance in. Will be set to `nil` if the database does not contain a matching instance.
        ///   - didLoad: An optional binding that will be set to `true` after the **first** load of the specified instance has completed.
        ///   - primaryKey: The single-column primary-key value to match.
        ///
        /// See also: ``bind(from:to:didLoad:multicolumnPrimaryKey:)`` and ``bind(from:to:didLoad:id:)``.
        public func bind(from database: Blackbird.Database?, to instance: Binding<T?>, didLoad: Binding<Bool>? = nil, primaryKey: Any) {
            watchedPrimaryKeys = Blackbird.PrimaryKeyValues([ [try! Blackbird.Value.fromAny(primaryKey)] ])
            bind(from: database, to: instance, didLoad: didLoad)  { try await T.read(from: $0, multicolumnPrimaryKey: [primaryKey]) }
        }

        /// Update a binding with the current instance matching a multi-column primary-key value, and keep it updated over time.
        /// - Parameters:
        ///   - database: The database to read from and monitor for changes.
        ///   - instance: A binding to store the matching instance in. Will be set to `nil` if the database does not contain a matching instance.
        ///   - didLoad: An optional binding that will be set to `true` after the **first** load of the specified instance has completed.
        ///   - multicolumnPrimaryKey: The multi-column primary-key values to match.
        ///
        /// See also: ``bind(from:to:didLoad:primaryKey:)`` and ``bind(from:to:didLoad:id:)``.
        public func bind(from database: Blackbird.Database?, to instance: Binding<T?>, didLoad: Binding<Bool>? = nil, multicolumnPrimaryKey: [Any]) {
            watchedPrimaryKeys = Blackbird.PrimaryKeyValues([ multicolumnPrimaryKey.map { try! Blackbird.Value.fromAny($0) } ])
            bind(from: database, to: instance, didLoad: didLoad)  { try await T.read(from: $0, multicolumnPrimaryKey: multicolumnPrimaryKey) }
        }

        /// Update a binding with the current instance matching a single-column primary-key value named `"id"`, and keep it updated over time.
        /// - Parameters:
        ///   - database: The database to read from and monitor for changes.
        ///   - instance: A binding to store the matching instance in. Will be set to `nil` if the database does not contain a matching instance.
        ///   - didLoad: An optional binding that will be set to `true` after the **first** load of the specified instance has completed.
        ///   - id: The ID value to match, assuming the table has a single-column primary key named `"id"`.
        ///
        /// See also: ``bind(from:to:didLoad:primaryKey:)`` and ``bind(from:to:didLoad:multicolumnPrimaryKey:)`` .
        public func bind(from database: Blackbird.Database?, to instance: Binding<T?>, didLoad: Binding<Bool>? = nil, id: Any) {
            watchedPrimaryKeys = Blackbird.PrimaryKeyValues([ [try! Blackbird.Value.fromAny(id)] ])
            bind(from: database, to: instance, didLoad: didLoad) { try await T.read(from: $0, id: id) }
        }

        private func bind(from database: Blackbird.Database?, to instance: Binding<T?>, didLoad: Binding<Bool>? = nil, updater: @escaping ((_ db: Blackbird.Database) async throws -> T?)) {
            self.updater = updater
            self.changeDatabase(database)
            self._instance = instance
            if let didLoad { _didLoad = didLoad }
            enqueueUpdate()
        }

        private func update() async throws {
            let result = updater != nil && database != nil ? try await updater!(database!) : nil
            await MainActor.run {
                self.instance = result
                didLoad = true
            }
        }
        
        private func changeDatabase(_ newDatabase: Database?) {
            if newDatabase == database { return }
            database = newDatabase
            
            if let database {
                self.changePublisher = database.changeReporter.changePublisher(for: T.table.name(type: T.self)).sink { [weak self] changedPrimaryKeys in
                    guard let self, Blackbird.isRelevantPrimaryKeyChange(watchedPrimaryKeys: self.watchedPrimaryKeys, changedPrimaryKeys: changedPrimaryKeys) else { return }
                    self.enqueueUpdate()
                }
            } else {
                self.changePublisher = nil
            }
        }
        
        private func enqueueUpdate() {
            Task { do { try await self.update() } catch { print("[Blackbird.ModelInstanceUpdater<\(String(describing: T.self))>] ⚠️ Error updating: \(error.localizedDescription)") } }
        }
    }
}
