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
//  BlackbirdCache.swift
//  Created by Marco Arment on 11/17/22.
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

@preconcurrency import Foundation // @preconcurrency due to DispatchSource not being annotated

extension Blackbird.Database {
    public struct CachePerformanceMetrics: Sendable {
        public let hits: Int
        public let misses: Int
        public let writes: Int
        public let rowInvalidations: Int
        public let queryInvalidations: Int
        public let tableInvalidations: Int
        public let evictions: Int
        public let lowMemoryFlushes: Int
    }
    
    public func cachePerformanceMetricsByTableName() -> [String: CachePerformanceMetrics] { cache.performanceMetrics() }
    public func resetCachePerformanceMetrics(tableName: String) { cache.resetPerformanceMetrics(tableName: tableName) }
    
    public func debugPrintCachePerformanceMetrics() {
        print("===== Blackbird.Database cache performance metrics =====")
        for (tableName, metrics) in cache.performanceMetrics() {
            let totalRequests = metrics.hits + metrics.misses
            let hitPercentStr =
                totalRequests == 0 ? "0%" :
                "\(Int(100.0 * Double(metrics.hits) / Double(totalRequests)))%"
                
            print("\(tableName): \(metrics.hits) hits (\(hitPercentStr)), \(metrics.misses) misses, \(metrics.writes) writes, \(metrics.rowInvalidations) row invalidations, \(metrics.queryInvalidations) query invalidations, \(metrics.tableInvalidations) table invalidations, \(metrics.evictions) evictions, \(metrics.lowMemoryFlushes) low-memory flushes")
        }
    }

    internal final class Cache: Sendable {
        private class CacheEntry<T: Sendable> {
            typealias AccessTime = UInt64
            private let _value: T
            var lastAccessed: AccessTime
            
            init(_ value: T) {
                _value = value
                lastAccessed = mach_absolute_time()
            }
            
            public func value() -> T {
                lastAccessed = mach_absolute_time()
                return _value
            }
        }

        internal enum CachedQueryResult: Sendable {
            case miss
            case hit(value: Sendable?)
        }

        private let lowMemoryEventSource: DispatchSourceMemoryPressure
        public init() {
            lowMemoryEventSource = DispatchSource.makeMemoryPressureSource(eventMask: [.warning, .critical])
            lowMemoryEventSource.setEventHandler { [weak self] in
                self?.entriesByTableName.withLock { entries in
                    //
                    // To avoid loading potentially-compressed memory pages and exacerbating memory pressure,
                    //  or taking precious time to walk the cache contents with the normal prune() operation,
                    //  just dump everything.
                    //
                    for (_, cache) in entries { cache.flushForLowMemory() }
                }
            }
            lowMemoryEventSource.resume()
        }
        
        deinit {
            lowMemoryEventSource.cancel()
        }
    
        private final class TableCache: @unchecked Sendable { /* unchecked due to internal locking */
            private let lock = Blackbird.Lock()
        
            // Cached data
            private var modelsByPrimaryKey: [Blackbird.Value: CacheEntry<any BlackbirdModel>] = [:]
            private var cachedQueries: [[Blackbird.Value]: CacheEntry<Sendable>] = [:]
            
            // Performance counters
            private var hits: Int = 0
            private var misses: Int = 0
            private var writes: Int = 0
            private var rowInvalidations: Int = 0
            private var queryInvalidations: Int = 0
            private var tableInvalidations: Int = 0
            private var evictions: Int = 0
            private var lowMemoryFlushes: Int = 0
            
            func get(primaryKey: Blackbird.Value) -> (any BlackbirdModel)? {
                lock.lock()
                defer { lock.unlock() }

                if let hit = modelsByPrimaryKey[primaryKey] {
                    hit.lastAccessed = mach_absolute_time()
                    hits += 1
                    return hit.value()
                } else {
                    misses += 1
                    return nil
                }
            }
            
            func get(primaryKeys: [Blackbird.Value]) -> (hits: [any BlackbirdModel], missedKeys: [Blackbird.Value]) {
                lock.lock()
                defer { lock.unlock() }

                var hitResults: [any BlackbirdModel] = []
                var missedKeys: [Blackbird.Value] = []
                for key in primaryKeys {
                    if let hit = modelsByPrimaryKey[key] { hitResults.append(hit.value()) } else { missedKeys.append(key) }
                }
                hits += hitResults.count
                misses += missedKeys.count
                return (hits: hitResults, missedKeys: missedKeys)
            }
            
            func getQuery(cacheKey: [Blackbird.Value]) -> CachedQueryResult {
                lock.lock()
                defer { lock.unlock() }

                if let hit = cachedQueries[cacheKey] {
                    hits += 1
                    return .hit(value: hit.value())
                } else {
                    misses += 1
                    return .miss
                }
            }
            
            func add(primaryKey: Blackbird.Value, instance: any BlackbirdModel, pruneToLimit: Int? = nil) {
                lock.lock()
                defer { lock.unlock() }

                modelsByPrimaryKey[primaryKey] = CacheEntry(instance)
                writes += 1
                
                if let pruneToLimit {
                    inLock_prune(entryLimit: pruneToLimit)
                }
            }
            
            func addQuery(cacheKey: [Blackbird.Value], result: Sendable?, pruneToLimit: Int? = nil) {
                lock.lock()
                defer { lock.unlock() }

                cachedQueries[cacheKey] = CacheEntry(result)
                writes += 1

                if let pruneToLimit {
                    inLock_prune(entryLimit: pruneToLimit)
                }
            
            }

            func delete(primaryKey: Blackbird.Value) {
                lock.lock()
                defer { lock.unlock() }

                modelsByPrimaryKey.removeValue(forKey: primaryKey)
                writes += 1
            }

            private func inLock_prune(entryLimit: Int) {
                if modelsByPrimaryKey.count + cachedQueries.count <= entryLimit { return }
                
                // As a table hits its entry limit, to avoid running the expensive pruning operation after EVERY addition,
                //  we prune the cache to HALF of its size limit to give it some headroom until the next prune is needed.
                let pruneToEntryLimit = entryLimit / 2
                
                if pruneToEntryLimit < 1 {
                    modelsByPrimaryKey.removeAll()
                    cachedQueries.removeAll()
                    return
                }

                var accessTimes: [CacheEntry.AccessTime] = []
                for (_, entry) in modelsByPrimaryKey { accessTimes.append(entry.lastAccessed) }
                for (_, entry) in cachedQueries      { accessTimes.append(entry.lastAccessed) }
                accessTimes.sort(by: >)
                
                let evictionCount = accessTimes.count - pruneToEntryLimit
                guard evictionCount > 0 else { return }
                let accessTimeThreshold = accessTimes[pruneToEntryLimit]
                modelsByPrimaryKey = modelsByPrimaryKey.filter { (key, value) in value.lastAccessed > accessTimeThreshold }
                cachedQueries      = cachedQueries.filter      { (key, value) in value.lastAccessed > accessTimeThreshold }
                evictions += evictionCount
            }
            
            func invalidate(primaryKeyValue: Blackbird.Value? = nil) {
                lock.lock()
                defer { lock.unlock() }

                if let primaryKeyValue {
                    if nil != modelsByPrimaryKey.removeValue(forKey: primaryKeyValue) {
                        rowInvalidations += 1
                    }
                } else {
                    if !modelsByPrimaryKey.isEmpty {
                        modelsByPrimaryKey.removeAll()
                        tableInvalidations += 1
                    }
                }
                
                if !cachedQueries.isEmpty {
                    cachedQueries.removeAll()
                    queryInvalidations += 1
                }
            }
            
            func flushForLowMemory() {
                lock.lock()
                defer { lock.unlock() }

                modelsByPrimaryKey.removeAll(keepingCapacity: false)
                cachedQueries.removeAll(keepingCapacity: false)
                lowMemoryFlushes += 1
            }
            
            func resetPerformanceMetrics() {
                lock.lock()
                defer { lock.unlock() }

                hits = 0
                misses = 0
                writes = 0
                evictions = 0
                rowInvalidations = 0
                queryInvalidations = 0
                tableInvalidations = 0
                lowMemoryFlushes = 0
            }
            
            func getPerformanceMetrics() -> CachePerformanceMetrics {
                lock.lock()
                defer { lock.unlock() }

                return CachePerformanceMetrics(hits: hits, misses: misses, writes: writes, rowInvalidations: rowInvalidations, queryInvalidations: queryInvalidations, tableInvalidations: tableInvalidations, evictions: evictions, lowMemoryFlushes: lowMemoryFlushes)
            }
        }
    
        private let entriesByTableName = Blackbird.Locked<[String: TableCache]>([:])
    
        internal func invalidate(tableName: String? = nil, primaryKeyValue: Blackbird.Value? = nil) {
            entriesByTableName.withLock {
                if let tableName {
                    $0[tableName]?.invalidate(primaryKeyValue: primaryKeyValue)
                } else {
                    for (_, entry) in $0 { entry.invalidate() }
                }
            }
        }
        
        internal func readModel(tableName: String, primaryKey: Blackbird.Value) -> (any BlackbirdModel)? {
            entriesByTableName.withLock {
                let tableCache: TableCache
                if let existingCache = $0[tableName] { tableCache = existingCache }
                else {
                    tableCache = TableCache()
                    $0[tableName] = tableCache
                }
                
                return tableCache.get(primaryKey: primaryKey)
            }
        }

        internal func readModels(tableName: String, primaryKeys: [Blackbird.Value]) -> (hits: [any BlackbirdModel], missedKeys: [Blackbird.Value]) {
            entriesByTableName.withLock {
                let tableCache: TableCache
                if let existingCache = $0[tableName] { tableCache = existingCache }
                else {
                    tableCache = TableCache()
                    $0[tableName] = tableCache
                }
            
                return tableCache.get(primaryKeys: primaryKeys)
            }
        }

        internal func writeModel(tableName: String, primaryKey: Blackbird.Value, instance: any BlackbirdModel, entryLimit: Int) {
            entriesByTableName.withLock {
                let tableCache: TableCache
                if let existingCache = $0[tableName] { tableCache = existingCache }
                else {
                    tableCache = TableCache()
                    $0[tableName] = tableCache
                }
                
                tableCache.add(primaryKey: primaryKey, instance: instance, pruneToLimit: entryLimit)
            }
        }

        internal func deleteModel(tableName: String, primaryKey: Blackbird.Value) {
            entriesByTableName.withLock {
                let tableCache: TableCache
                if let existingCache = $0[tableName] { tableCache = existingCache }
                else {
                    tableCache = TableCache()
                    $0[tableName] = tableCache
                }
                
                tableCache.delete(primaryKey: primaryKey)
            }
        }

        internal func readQueryResult(tableName: String, cacheKey: [Blackbird.Value]) -> CachedQueryResult {
            entriesByTableName.withLock {
                let tableCache: TableCache
                if let existingCache = $0[tableName] { tableCache = existingCache }
                else {
                    tableCache = TableCache()
                    $0[tableName] = tableCache
                }
                return tableCache.getQuery(cacheKey: cacheKey)
            }
        }

        internal func writeQueryResult(tableName: String, cacheKey: [Blackbird.Value], result: Sendable, entryLimit: Int) {
            entriesByTableName.withLock {
                let tableCache: TableCache
                if let existingCache = $0[tableName] { tableCache = existingCache }
                else {
                    tableCache = TableCache()
                    $0[tableName] = tableCache
                }
                
                tableCache.addQuery(cacheKey: cacheKey, result: result, pruneToLimit: entryLimit)
            }
        }
        
        internal func performanceMetrics() -> [String: CachePerformanceMetrics] {
            entriesByTableName.withLock { tableCaches in
                tableCaches.mapValues { $0.getPerformanceMetrics() }
            }
        }

        internal func resetPerformanceMetrics(tableName: String) {
            entriesByTableName.withLock { $0[tableName]?.resetPerformanceMetrics() }
        }
    }
}


extension BlackbirdModel {
    internal func _saveCachedInstance(for database: Blackbird.Database) {
        let cacheLimit = Self.cacheLimit
        if cacheLimit > 0, let pkValues = try? self.primaryKeyValues(), pkValues.count == 1, let pk = try? Blackbird.Value.fromAny(pkValues.first!) {
            database.cache.writeModel(tableName: Self.tableName, primaryKey: pk, instance: self, entryLimit: cacheLimit)
        }
    }

    internal func _deleteCachedInstance(for database: Blackbird.Database) {
        if Self.cacheLimit > 0, let pkValues = try? self.primaryKeyValues(), pkValues.count == 1, let pk = try? Blackbird.Value.fromAny(pkValues.first!) {
            database.cache.deleteModel(tableName: Self.tableName, primaryKey: pk)
        }
    }

    internal static func _cachedInstance(for database: Blackbird.Database, primaryKeyValue: Blackbird.Value) -> Self? {
        guard Self.cacheLimit > 0 else { return nil }
        return database.cache.readModel(tableName: Self.tableName, primaryKey: primaryKeyValue) as? Self
    }

    internal static func _cachedInstances(for database: Blackbird.Database, primaryKeyValues: [Blackbird.Value]) -> (hits: [Self], missedKeys: [Blackbird.Value]) {
        guard Self.cacheLimit > 0 else { return (hits: [], missedKeys: primaryKeyValues) }
        let results = database.cache.readModels(tableName: Self.tableName, primaryKeys: primaryKeyValues)

        var hits: [Self] = []
        for hit in results.hits {
            guard let hit = hit as? Self else { return (hits: [], missedKeys: primaryKeyValues) }
            hits.append(hit)
        }
        return (hits: hits, missedKeys: results.missedKeys)
    }
}
