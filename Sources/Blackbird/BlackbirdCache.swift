//
//  BlackbirdCache.swift
//  Created by Marco Arment on 11/17/22.
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

extension Blackbird.Database {
    public struct CachePerformanceMetrics: Sendable {
        public let hits: Int
        public let misses: Int
        public let writes: Int
        public let rowInvalidations: Int
        public let tableInvalidations: Int
    }
    
    public func cachePerformanceMetricsByTableName() -> [String : CachePerformanceMetrics] { cache.performanceMetrics() }
    public func resetCachePerformanceMetrics(tableName: String) { cache.resetPerformanceMetrics(tableName: tableName) }
    
    public func debugPrintCachePerformanceMetrics() {
        print("===== Blackbird.Database cache performance metrics =====")
        for (tableName, metrics) in cache.performanceMetrics() {
            let totalRequests = metrics.hits + metrics.misses
            let hitPercentStr =
                totalRequests == 0 ? "0%" :
                "\(Int(100.0 * Double(metrics.hits) / Double(totalRequests)))%"
                
            print("\(tableName): \(metrics.hits) hits (\(hitPercentStr)), \(metrics.misses) misses, \(metrics.writes) writes, \(metrics.rowInvalidations) row invalidations, \(metrics.tableInvalidations) table invalidations")
        }
    }

    internal final class Cache: Sendable {
        private final class TableCache {
            // Cached data
            var modelsByPrimaryKey: [Blackbird.Value : any BlackbirdModel] = [:]
            
            // Performance counters
            var hits: Int = 0
            var misses: Int = 0
            var writes: Int = 0
            var rowInvalidations: Int = 0
            var tableInvalidations: Int = 0
            
            func invalidate(primaryKeyValue: Blackbird.Value? = nil) {
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
            }
            
            func resetPerformanceMetrics() {
                hits = 0
                misses = 0
                writes = 0
                rowInvalidations = 0
                tableInvalidations = 0
            }
        }
    
        private let entriesByTableName = Blackbird.Locked<[String : TableCache]>([:])
    
        internal func invalidate(tableName: String? = nil, primaryKeyValue: Blackbird.Value? = nil) {
            entriesByTableName.withLock {
                if let tableName {
//                    print("[cache] invalidating \(tableName):\(primaryKeyValue ?? "all")")
                    $0[tableName]?.invalidate(primaryKeyValue: primaryKeyValue)
                } else {
//                    print("[cache] invalidating entire database")
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
                    tableCache.misses += 1
                    return nil
                }

                if let hit = tableCache.modelsByPrimaryKey[primaryKey] {
                    tableCache.hits += 1
                    return hit
                } else {
                    tableCache.misses += 1
                    return nil
                }
            }
        }

        internal func readModels(tableName: String, primaryKeys: [Blackbird.Value]) -> (hits: [any BlackbirdModel], missedKeys: [Blackbird.Value]) {
            entriesByTableName.withLock {
                let tableCache: TableCache
                if let existingCache = $0[tableName] { tableCache = existingCache }
                else {
                    tableCache = TableCache()
                    $0[tableName] = tableCache
                    tableCache.misses += primaryKeys.count
                    return (hits: [], missedKeys: primaryKeys)
                }
            
                var hits: [any BlackbirdModel] = []
                var missedKeys: [Blackbird.Value] = []
                for key in primaryKeys {
                    if let hit = tableCache.modelsByPrimaryKey[key] { hits.append(hit) } else { missedKeys.append(key) }
                }
                tableCache.hits += hits.count
                tableCache.misses += missedKeys.count
                return (hits: hits, missedKeys: missedKeys)
            }
        }

        internal func writeModel(tableName: String, primaryKey: Blackbird.Value, instance: any BlackbirdModel) {
            entriesByTableName.withLock {
                let tableCache: TableCache
                if let existingCache = $0[tableName] { tableCache = existingCache }
                else {
                    tableCache = TableCache()
                    $0[tableName] = tableCache
                }

                tableCache.modelsByPrimaryKey[primaryKey] = instance
                tableCache.writes += 1
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

                tableCache.modelsByPrimaryKey.removeValue(forKey: primaryKey)
                tableCache.writes += 1
            }
        }
        
        internal func performanceMetrics() -> [String : CachePerformanceMetrics] {
            entriesByTableName.withLock { tableCaches in
                tableCaches.mapValues { CachePerformanceMetrics(hits: $0.hits, misses: $0.misses, writes: $0.writes, rowInvalidations: $0.rowInvalidations, tableInvalidations: $0.tableInvalidations) }
            }
        }

        internal func resetPerformanceMetrics(tableName: String) {
            entriesByTableName.withLock { $0[tableName]?.resetPerformanceMetrics() }
        }
    }
}


extension BlackbirdModel {
    internal func _saveCachedInstance(for database: Blackbird.Database) {
        if Self.enableCaching, let pkValues = try? self.primaryKeyValues(), pkValues.count == 1, let pk = try? Blackbird.Value.fromAny(pkValues.first!) {
            database.cache.writeModel(tableName: Self.tableName, primaryKey: pk, instance: self)
        }
    }

    internal func _deleteCachedInstance(for database: Blackbird.Database) {
        if Self.enableCaching, let pkValues = try? self.primaryKeyValues(), pkValues.count == 1, let pk = try? Blackbird.Value.fromAny(pkValues.first!) {
            database.cache.deleteModel(tableName: Self.tableName, primaryKey: pk)
        }
    }

    internal static func _cachedInstance(for database: Blackbird.Database, primaryKeyValue: Blackbird.Value) -> Self? {
        guard Self.enableCaching else { return nil }
        return database.cache.readModel(tableName: Self.tableName, primaryKey: primaryKeyValue) as? Self
    }

    internal static func _cachedInstances(for database: Blackbird.Database, primaryKeyValues: [Blackbird.Value]) -> (hits: [Self], missedKeys: [Blackbird.Value]) {
        guard Self.enableCaching else { return (hits: [], missedKeys: primaryKeyValues) }
        let results = database.cache.readModels(tableName: Self.tableName, primaryKeys: primaryKeyValues)

        var hits: [Self] = []
        for hit in results.hits {
            guard let hit = hit as? Self else { return (hits: [], missedKeys: primaryKeyValues) }
            hits.append(hit)
        }
        return (hits: hits, missedKeys: results.missedKeys)
    }
}
