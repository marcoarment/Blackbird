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

extension Blackbird {
    /// The results wrapper for @BlackbirdLiveQuery and @BlackbirdLiveModels.
    public struct LiveResults<T: Sendable>: Sendable, Equatable where T: Equatable {
        public static func == (lhs: Blackbird.LiveResults<T>, rhs: Blackbird.LiveResults<T>) -> Bool { lhs.didLoad == rhs.didLoad && lhs.results == rhs.results }
        
        /// The latest results fetched.
        public var results: [T] = []
        
        /// Whether this result set has **ever** completed loading.
        ///
        /// When used by ``BlackbirdLiveModels`` or ``BlackbirdLiveQuery``, this will only be set to `false` during their initial load.
        /// It will **not** be set to `false` during subsequent updates triggered by changes to the underlying database.
        public var didLoad = false
        
        public init(results: [T] = [], didLoad: Bool = false) {
            self.results = results
            self.didLoad = didLoad
        }
    }
}

// MARK: - Fetch property wrappers

/// An array of database rows produced by a generator function, kept up-to-date as data changes in the specified table.
///
/// Set `@Environment(\.blackbirdDatabase)` to the desired database instance to read.
///
/// The generator is passed the current database as its sole argument (`$0`).
///
/// ## Example
///
/// ```swift
/// @BlackbirdLiveQuery(tableName: "Post", {
///     try await $0.query("SELECT COUNT(*) AS c FROM Post")
/// }) var count
/// ```
///
/// `count` is a ``Blackbird/LiveResults`` object:
/// * `count.results.first["c"]` will be the resulting ``Blackbird/Value``
/// * `count.didLoad` will be `false` during the initial load (useful for displaying a loading state in the UI)
///
@propertyWrapper public struct BlackbirdLiveQuery: DynamicProperty {
    @State private var results = Blackbird.LiveResults<Blackbird.Row>()
    @Environment(\.blackbirdDatabase) var environmentDatabase

    public var wrappedValue: Blackbird.LiveResults<Blackbird.Row> {
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
        queryUpdater.bind(from: environmentDatabase, tableName: tableName, to: $results, generator: generator)
    }
}

/// An array of ``BlackbirdModel`` instances produced by a generator function, kept up-to-date as their table's data changes in the database.
///
/// Set `@Environment(\.blackbirdDatabase)` to the desired database instance to read.
///
/// The generator is passed the current database as its sole argument (`$0`).
///
/// ## Example
///
/// ```swift
/// @BlackbirdLiveModels({
///     try await Post.read(from: $0, where: "id > 3 ORDER BY date")
/// }) var posts
/// ```
///
/// `posts` is a ``Blackbird/LiveResults`` object:
/// * `posts.results` will be an array of Post models matching the query
/// * `posts.didLoad` will be `false` during the initial load (useful for displaying a loading state in the UI)
///
@propertyWrapper public struct BlackbirdLiveModels<T: BlackbirdModel>: DynamicProperty {
    @State private var result = Blackbird.LiveResults<T>()
    @Environment(\.blackbirdDatabase) var environmentDatabase
    
    public var wrappedValue: Blackbird.LiveResults<T> {
        get { result }
        set { }
    }
    
    private let queryUpdater = Blackbird.ModelArrayUpdater<T>()
    private let generator: Blackbird.CachedResultGenerator<[T]>

    public init(_ generator: @escaping Blackbird.CachedResultGenerator<[T]>) {
        self.generator = generator
    }

    public func update() {
        queryUpdater.bind(from: environmentDatabase, to: $result, generator: generator)
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
    
    private let queryUpdater: Blackbird.ModelUpdater<T>
    private var generator: Blackbird.CachedResultGenerator<T?>?

    public init(_ instance: T) {
        _instance = State(initialValue: instance)
        queryUpdater = Blackbird.ModelUpdater<T>(initialValue: instance)

        do {
            let primaryKeyValues = try instance.primaryKeyValues()
            generator = { try await T.read(from: $0, multicolumnPrimaryKey: primaryKeyValues) }
        } catch {
            print("[Blackbird.BlackbirdLiveModel<\(String(describing: T.self))>] ⚠️ Error getting primary key values: \(error.localizedDescription)")
        }
    }

    public func update() {
        queryUpdater.bind(from: environmentDatabase, to: $instance, generator: generator)
    }
}

extension BlackbirdModel {
    public var liveModel: BlackbirdLiveModel<Self> { get { BlackbirdLiveModel(self) } }
    public typealias LiveResults = Blackbird.LiveResults<Self>
}

// MARK: - Multi-row query updaters

extension Blackbird {
    public final class QueryUpdater: @unchecked Sendable { // unchecked due to internal locking
        @Binding public var results: Blackbird.LiveResults<Blackbird.Row>

        private let resultPublisher = CachedResultPublisher<[Blackbird.Row]>()
        private var changePublishers: [AnyCancellable] = []
        private let lock = Blackbird.Lock()

        public init() {
            _results = Binding<Blackbird.LiveResults<Blackbird.Row>>(get: { Blackbird.LiveResults<Blackbird.Row>() }, set: { _ in })
        }
        
        public func bind(from database: Blackbird.Database?, tableName: String, to results: Binding<Blackbird.LiveResults<Blackbird.Row>>, generator: CachedResultGenerator<[Blackbird.Row]>?) {
            lock.lock()
            defer { lock.unlock() }
        
            changePublishers.removeAll()
            resultPublisher.subscribe(to: tableName, in: database, generator: generator)
            _results = results
            
            changePublishers.append(resultPublisher.valuePublisher.sink { [weak self] value in
                guard let self else { return }
                let results: Blackbird.LiveResults<Blackbird.Row>
                if let value {
                    results = Blackbird.LiveResults<Blackbird.Row>(results: value, didLoad: true)
                } else {
                    results = Blackbird.LiveResults<Blackbird.Row>(results: [], didLoad: false)
                }
                
                DispatchQueue.main.async { // kicking this to the next runloop to prevent state updates from happening while building the view
                    self.results = results
                }
            })
        }
    }

    public final class ModelArrayUpdater<T: BlackbirdModel>: @unchecked Sendable { // unchecked due to internal locking
        @Binding public var results: Blackbird.LiveResults<T>

        private let resultPublisher: CachedResultPublisher<[T]>
        private var changePublishers: [AnyCancellable] = []
        private let lock = Blackbird.Lock()

        public init(initialValue: [T]? = nil) {
            _results = Binding<Blackbird.LiveResults<T>>(get: { Blackbird.LiveResults<T>(results: initialValue ?? [], didLoad: initialValue != nil) }, set: { _ in })
            resultPublisher = CachedResultPublisher<[T]>(initialValue: initialValue)
        }
        
        public func bind(from database: Blackbird.Database?, to results: Binding<Blackbird.LiveResults<T>>, generator: CachedResultGenerator<[T]>?) {
            lock.lock()
            defer { lock.unlock() }
            
            changePublishers.removeAll()
            resultPublisher.subscribe(to: T.table.name, in: database, generator: generator)
            _results = results
            
            changePublishers.append(resultPublisher.valuePublisher.sink { [weak self] value in
                guard let self else { return }
                DispatchQueue.main.async {
                    if let value {
                        self.results = Blackbird.LiveResults<T>(results: value, didLoad: true)
                    } else {
                        self.results = Blackbird.LiveResults<T>(results: [], didLoad: false)
                    }
                }
            })
        }
    }

    public class ModelUpdater<T: BlackbirdModel>: @unchecked Sendable  { // unchecked due to internal locking
        @Binding public var instance: T?

        private let resultPublisher: CachedResultPublisher<T?>
        private var changePublishers: [AnyCancellable] = []
        private let lock = Blackbird.Lock()

        public init(initialValue: T? = nil) {
            _instance = Binding<T?>(get: { initialValue }, set: { _ in })
            resultPublisher = CachedResultPublisher<T?>(initialValue: initialValue)
        }
        
        public func bind(from database: Blackbird.Database?, to instance: Binding<T?>, generator: CachedResultGenerator<T?>?) {
            lock.lock()
            defer { lock.unlock() }

            changePublishers.removeAll()
            resultPublisher.subscribe(to: T.table.name, in: database, generator: generator)
            _instance = instance
            changePublishers.append(resultPublisher.valuePublisher.sink { [weak self] value in
                guard let self else { return }
                DispatchQueue.main.async {
                    self.instance = value ?? nil
                }
            })
        }
    }

}


// MARK: - Single-instance updater

extension Blackbird {
    public final class ModelInstanceUpdater<T: BlackbirdModel>: @unchecked Sendable { // unchecked due to internal locking
        @Binding public var instance: T?
        @Binding public var didLoad: Bool
        
        private var database: Blackbird.Database? = nil
        private var updater: ((_ db: Blackbird.Database) async throws -> T?)? = nil
        private var changePublisher: AnyCancellable? = nil
        private var watchedPrimaryKeys = Blackbird.PrimaryKeyValues()
        private let lock = Blackbird.Lock()

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
        public func bind(from database: Blackbird.Database?, to instance: Binding<T?>, didLoad: Binding<Bool>? = nil, primaryKey: Sendable) {
            lock.lock()
            watchedPrimaryKeys = Blackbird.PrimaryKeyValues([ [try! Blackbird.Value.fromAny(primaryKey)] ])
            lock.unlock()
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
        public func bind(from database: Blackbird.Database?, to instance: Binding<T?>, didLoad: Binding<Bool>? = nil, multicolumnPrimaryKey: [Sendable]) {
            lock.lock()
            watchedPrimaryKeys = Blackbird.PrimaryKeyValues([ multicolumnPrimaryKey.map { try! Blackbird.Value.fromAny($0) } ])
            lock.unlock()
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
        public func bind(from database: Blackbird.Database?, to instance: Binding<T?>, didLoad: Binding<Bool>? = nil, id: Sendable) {
            lock.lock()
            watchedPrimaryKeys = Blackbird.PrimaryKeyValues([ [try! Blackbird.Value.fromAny(id)] ])
            lock.unlock()
            bind(from: database, to: instance, didLoad: didLoad) { try await T.read(from: $0, id: id) }
        }

        private func bind(from database: Blackbird.Database?, to instance: Binding<T?>, didLoad: Binding<Bool>? = nil, updater: @escaping ((_ db: Blackbird.Database) async throws -> T?)) {
            lock.lock()
            self.updater = updater
            self.changeDatabase(database)
            self._instance = instance
            if let didLoad { _didLoad = didLoad }
            lock.unlock()
            
            enqueueUpdate()
        }

        private func update() async throws {
            let result = updater != nil && database != nil ? try await updater!(database!) : nil
            await MainActor.run {
                lock.lock()
                defer { lock.unlock() }
                self.instance = result
                didLoad = true
            }
        }
        
        private func changeDatabase(_ newDatabase: Database?) {
            if newDatabase == database { return }
            database = newDatabase
            
            if let database {
                self.changePublisher = database.changeReporter.changePublisher(for: T.table.name).sink { [weak self] change in
                    guard let self, Blackbird.isRelevantPrimaryKeyChange(watchedPrimaryKeys: self.watchedPrimaryKeys, changedPrimaryKeys: change.primaryKeys) else { return }
                    self.enqueueUpdate()
                }
            } else {
                self.changePublisher = nil
            }
        }
        
        private func enqueueUpdate() {
            Task {
                do { try await self.update() }
                catch { print("[Blackbird.ModelInstanceUpdater<\(String(describing: T.self))>] ⚠️ Error updating: \(error.localizedDescription)") }
            }
        }
    }
}
