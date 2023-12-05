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
//  BlackbirdObservation.swift
//  Created by Marco Arment on 12/3/23.
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

import Observation
import Combine

// MARK: - BlackbirdModelQueryObserver

@available(macOS 14, iOS 17, tvOS 17, watchOS 10, *)
extension BlackbirdModel {
    public typealias QueryObserver<R> = BlackbirdModelQueryObserver<Self, R>
}

@available(macOS 14, iOS 17, tvOS 17, watchOS 10, *)
@Observable
public final class BlackbirdModelQueryObserver<T: BlackbirdModel, R: Any> {
    /// Whether this instance is currently loading from the database, either initially or after an update.
    public var isLoading = false
    
    /// Whether this instance has ever loaded from the database. After the initial load, it is set to `true` and remains true.
    public var didLoad = false
    
    /// The current instance matching the supplied primary-key value.
    public var result: R?

    @ObservationIgnored private weak var database: Blackbird.Database?
    @ObservationIgnored private var observer: AnyCancellable? = nil
    
    @ObservationIgnored private var fetcher: ((_ database: Blackbird.Database) async throws -> R)
    
    public init(in database: Blackbird.Database? = nil, _ fetcher: @escaping ((_ database: Blackbird.Database) async throws -> R)) {
        self.fetcher = fetcher
        bind(to: database)
    }

    /// Set or change the ``Blackbird/Database`` to read from and monitor for changes.
    public func bind(to database: Blackbird.Database?) {
        guard let database else { return }
        if let oldValue = self.database, oldValue.id == database.id { return }
    
        self.database = database

        observer?.cancel()
        observer = nil
        result = nil
        
        observer = T.changePublisher(in: database).sink { _ in
            Task.detached { [weak self] in await self?.update() }
        }
        Task.detached { [weak self] in await self?.update() }
    }

    let updateSemaphore = Blackbird.Semaphore(value: 1)
    private func update() async {
        await updateSemaphore.wait()
        defer { updateSemaphore.signal() }
    
        await MainActor.run {
            self.isLoading = true
        }
        
        let result: R? = if let database { try? await fetcher(database) } else { nil }

        await MainActor.run {
            self.result = result
            self.isLoading = false
            if !self.didLoad { self.didLoad = true }
        }
    }
}

// MARK: - BlackbirdModelObserver

@available(macOS 14, iOS 17, tvOS 17, watchOS 10, *)
extension BlackbirdModel {
    public typealias Observer = BlackbirdModelObserver<Self>
    
    public var observer: Observer { Observer(multicolumnPrimaryKey: try! primaryKeyValues()) }
}

@available(macOS 14, iOS 17, tvOS 17, watchOS 10, *)
@Observable
public final class BlackbirdModelObserver<T: BlackbirdModel> {
    /// Whether this instance is currently loading from the database, either initially or after an update.
    public var isLoading = false
    
    /// Whether this instance has ever loaded from the database. After the initial load, it is set to `true` and remains true.
    public var didLoad = false
    
    /// The current instance matching the supplied primary-key value.
    public var instance: T?

    @ObservationIgnored private weak var database: Blackbird.Database?
    @ObservationIgnored private var multicolumnPrimaryKey: [Blackbird.Value]?
    @ObservationIgnored private var observer: AnyCancellable? = nil
    
    /// Initializer to track a single-column primary-key value.
    public convenience init(in database: Blackbird.Database? = nil, primaryKey: Sendable? = nil) {
        self.init(in: database, multicolumnPrimaryKey: [primaryKey])
    }
    
    /// Initializer to track a multi-column primary-key value.
    public init(in database: Blackbird.Database? = nil, multicolumnPrimaryKey: [Sendable]? = nil) {
        self.multicolumnPrimaryKey = multicolumnPrimaryKey?.map { try! Blackbird.Value.fromAny($0) } ?? nil
        bind(to: database)
    }
    
    /// Set or change the ``Blackbird/Database`` to read from and monitor for changes.
    public func bind(to database: Blackbird.Database?) {
        guard let database else { return }
        if let oldValue = self.database, oldValue.id == database.id { return }
    
        self.database = database
        updateDatabaseObserver()
    }
    
    /// Set or change the single-column primary-key value to observe.
    public func observe(primaryKey: Sendable? = nil) { observe(multicolumnPrimaryKey: [primaryKey]) }

    /// Set or change the multi-column primary-key value to observe.
    public func observe(multicolumnPrimaryKey: [Sendable]? = nil) {
        let multicolumnPrimaryKey = multicolumnPrimaryKey?.map { try! Blackbird.Value.fromAny($0) } ?? nil
        if multicolumnPrimaryKey == self.multicolumnPrimaryKey { return }
        
        self.multicolumnPrimaryKey = multicolumnPrimaryKey
        updateDatabaseObserver()
    }

    private func updateDatabaseObserver() {
        observer?.cancel()
        observer = nil
        instance = nil
        
        guard let database, let multicolumnPrimaryKey else { return }
        
        observer = T.changePublisher(in: database, multicolumnPrimaryKey: multicolumnPrimaryKey).sink { _ in
            Task.detached { [weak self] in await self?.update() }
        }
        Task.detached { [weak self] in await self?.update() }
    }
    
    let updateSemaphore = Blackbird.Semaphore(value: 1)
    private func update() async {
        await updateSemaphore.wait()
        defer { updateSemaphore.signal() }
    
        await MainActor.run {
            self.isLoading = true
        }
        
        let newInstance: T? = if let database, let multicolumnPrimaryKey { try? await T.read(from: database, multicolumnPrimaryKey: multicolumnPrimaryKey) } else { nil }

        await MainActor.run {
            self.instance = newInstance
            self.isLoading = false
            if !self.didLoad { self.didLoad = true }
        }
    }
}

