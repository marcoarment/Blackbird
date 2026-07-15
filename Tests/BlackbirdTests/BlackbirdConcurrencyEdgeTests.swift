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
//  BlackbirdConcurrencyEdgeTests.swift
//
//  Edge-case tests for concurrency, transactions, caching, and change
//  reporting: deep savepoint nesting, the async-transaction barrier,
//  cache coherence across commit/rollback and raw-SQL bypass, publisher
//  batching/filtering, and the Semaphore/Locked primitives.
//

import XCTest
import Combine
@testable import Blackbird

// MARK: - Models

struct EdgeConcItem: BlackbirdModel {
    static let tableName = "EdgeConcItem"
    static let cacheLimit: Int = 0

    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var title: String
    @BlackbirdColumn var num: Int
}

struct EdgeConcCachedItem: BlackbirdModel {
    static let tableName = "EdgeConcCachedItem"
    static let cacheLimit: Int = 100

    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var title: String
}

struct EdgeConcTinyCacheItem: BlackbirdModel {
    static let tableName = "EdgeConcTinyCacheItem"
    static let cacheLimit: Int = 1

    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var title: String
}

struct EdgeConcCounter: BlackbirdModel {
    static let tableName = "EdgeConcCounter"
    static let cacheLimit: Int = 0

    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var value: Int64
}

struct EdgeConcTableA: BlackbirdModel {
    static let tableName = "EdgeConcTableA"
    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var v: String
}

struct EdgeConcTableB: BlackbirdModel {
    static let tableName = "EdgeConcTableB"
    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var v: String
}

struct EdgeConcTableC: BlackbirdModel {
    static let tableName = "EdgeConcTableC"
    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var v: String
}

struct EdgeConcIgnorable: BlackbirdModel {
    static let tableName = "EdgeConcIgnorable"
    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var watched: String
    @BlackbirdColumn var ignored: String
}

// Not a model: used as a transaction return value.
struct EdgeConcPayload: Sendable, Equatable {
    let n: Int
    let s: String
}

// MARK: - Tests

final class BlackbirdConcurrencyEdgeTests: XCTestCase, @unchecked Sendable {
    enum Error: Swift.Error {
        case testError
    }

    var sqliteFilename = ""

    override func setUpWithError() throws {
        let dir = FileManager.default.temporaryDirectory.path
        sqliteFilename = "\(dir)/testEdgeConc\(Int64.random(in: 0..<Int64.max)).sqlite"
    }

    override func tearDownWithError() throws {
        if sqliteFilename != "", sqliteFilename != ":memory:", FileManager.default.fileExists(atPath: sqliteFilename) {
            for path in Blackbird.Database.allFilePaths(for: sqliteFilename) {
                try? FileManager.default.removeItem(atPath: path)
            }
        }
    }

    // MARK: - Transactions

    // Five levels of nested transactions; the level-3 transaction (containing
    // levels 4 and 5) is cancelled. Exactly the writes from levels 1, 2, and
    // level 2 after the rollback must survive — including after close+reopen.
    func testFiveLevelNestedTransactionsRollBackAtDepthThree() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)

        try await db.transaction { core in // depth 1
            try EdgeConcItem(id: 1, title: "depth 1", num: 1).write(to: core)

            try await db.transaction { core in // depth 2
                try EdgeConcItem(id: 2, title: "depth 2", num: 2).write(to: core)

                let depth3 = try await db.cancellableTransaction { core in // depth 3
                    try EdgeConcItem(id: 3, title: "depth 3", num: 3).write(to: core)

                    try await db.transaction { core in // depth 4
                        try EdgeConcItem(id: 4, title: "depth 4", num: 4).write(to: core)

                        try await db.transaction { core in // depth 5
                            try EdgeConcItem(id: 5, title: "depth 5", num: 5).write(to: core)
                        }
                    }
                    throw Blackbird.Error.cancelTransaction
                }
                guard case .rolledBack = depth3 else { return XCTFail("expected depth-3 rollback") }

                // Depth 2 must remain intact and writable after the inner rollback
                try EdgeConcItem(id: 22, title: "depth 2 after rollback", num: 22).write(to: core)
            }
        }

        let idsBeforeReopen = Set(try await EdgeConcItem.read(from: db, matching: .all).map(\.id))
        XCTAssertEqual(idsBeforeReopen, [1, 2, 22], "wrong rows survived nested rollback")

        // Durability: same rows after close and reopen
        await db.close()
        let reopened = try Blackbird.Database(path: sqliteFilename)
        let idsAfterReopen = Set(try await EdgeConcItem.read(from: reopened, matching: .all).map(\.id))
        XCTAssertEqual(idsAfterReopen, [1, 2, 22], "rows differ after close+reopen")
        await reopened.close()
    }

    // Transactions that do no database work, or only read, must commit cleanly
    // and leave the database fully usable.
    func testTransactionWithNoDatabaseWorkAndReadOnlyTransaction() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)

        try await db.transaction { _ in } // zero database work

        try await EdgeConcItem(id: 1, title: "readable", num: 0).write(to: db)

        let readTitle: String? = try await db.transaction { core in
            try EdgeConcItem.read(from: core, id: 1)?.title // read-only body
        }
        XCTAssertEqual(readTitle, "readable")

        // Still writable afterward
        try await EdgeConcItem(id: 2, title: "after", num: 0).write(to: db)
        let count = try await EdgeConcItem.count(in: db, matching: .all)
        XCTAssertEqual(count, 2)
        await db.close()
    }

    // Transactions must correctly return various Sendable types.
    func testTransactionReturnValueTypes() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)

        let payload: EdgeConcPayload = try await db.transaction { _ in EdgeConcPayload(n: 42, s: "answer") }
        XCTAssertEqual(payload, EdgeConcPayload(n: 42, s: "answer"))

        let someValue: Int? = try await db.transaction { _ in 99 }
        XCTAssertEqual(someValue, 99)

        let noneValue: String? = try await db.transaction { _ in nil as String? }
        XCTAssertNil(noneValue)

        let arrayValue: [Int64] = try await db.transaction { core in
            try EdgeConcItem(id: 11, title: "a", num: 0).write(to: core)
            try EdgeConcItem(id: 12, title: "b", num: 0).write(to: core)
            return try EdgeConcItem.read(from: core, sqlWhere: "1 ORDER BY id").map(\.id)
        }
        XCTAssertEqual(arrayValue, [11, 12])
        await db.close()
    }

    // A cancellable transaction must see its own uncommitted writes from inside,
    // and none of them from outside after the rollback.
    func testCancelledTransactionSeesOwnWritesInsideButNotAfter() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await EdgeConcItem(id: 1, title: "setup", num: 0).write(to: db)

        let insideTitle = Blackbird.Locked<String?>(nil)
        let insideCount = Blackbird.Locked(-1)
        let result = try await db.cancellableTransaction { core in
            try EdgeConcItem(id: 7, title: "uncommitted", num: 7).write(to: core)

            let inside = try EdgeConcItem.read(from: core, id: 7)
            insideTitle.value = inside?.title
            let rows = try core.query("SELECT COUNT(*) AS c FROM EdgeConcItem")
            insideCount.value = rows.first?["c"]?.intValue ?? -1
            throw Blackbird.Error.cancelTransaction
        }
        guard case .rolledBack = result else { return XCTFail("expected rollback") }

        XCTAssertEqual(insideTitle.value, "uncommitted", "transaction must see its own uncommitted write")
        XCTAssertEqual(insideCount.value, 2, "transaction's row count must include its own uncommitted write")

        let after = try await EdgeConcItem.read(from: db, id: 7)
        XCTAssertNil(after, "rolled-back write is still visible")
        let countAfter = try await EdgeConcItem.count(in: db, matching: .all)
        XCTAssertEqual(countAfter, 1)
        await db.close()
    }

    // A non-cancel error thrown from a nested inner transaction must roll back
    // only the inner savepoint; the outer transaction can catch it, continue,
    // and commit its own writes made both before and after the caught error.
    func testInnerTransactionErrorCaughtByOuterCommits() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)

        try await db.transaction { core in
            try EdgeConcItem(id: 1, title: "outer before", num: 0).write(to: core)

            do {
                try await db.transaction { core in
                    try EdgeConcItem(id: 2, title: "inner, must roll back", num: 0).write(to: core)
                    throw Error.testError
                }
                XCTFail("expected inner transaction to throw")
            } catch Error.testError { } // expected; outer continues

            try EdgeConcItem(id: 3, title: "outer after", num: 0).write(to: core)
        }

        let ids = Set(try await EdgeConcItem.read(from: db, matching: .all).map(\.id))
        XCTAssertEqual(ids, [1, 3], "inner failed transaction's write must be gone; outer's writes must persist")
        await db.close()
    }

    // DOCUMENTED SEMANTICS: an unstructured `Task { }` spawned inside a transaction
    // body inherits the transaction's TaskLocal ID (standard Swift task-local
    // inheritance), so its database work joins the transaction — passing the barrier,
    // landing inside the open savepoint, and rolling back with it. This cannot be
    // distinguished from awaited structured children (task groups, async let), which
    // MUST pass the barrier or the transaction would deadlock. Code that wants a
    // write independent of the surrounding transaction must use `Task.detached`
    // (verified by the next test), or simply not spawn from transaction bodies.
    func testUnstructuredTaskWriteJoinsTransactionAndRollsBack() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await EdgeConcItem(id: 1, title: "setup", num: 0).write(to: db) // resolve schema

        let unawaitedTask = Blackbird.Locked<Task<Void, Swift.Error>?>(nil)
        let result = try await db.cancellableTransaction { core in
            try EdgeConcItem(id: 50, title: "inside transaction", num: 0).write(to: core)

            unawaitedTask.value = Task {
                try await EdgeConcItem(id: 60, title: "fire and forget", num: 0).write(to: db)
            }

            // Suspend so the unawaited task gets a chance to run before the rollback
            try await Task.sleep(nanoseconds: 300_000_000)
            throw Blackbird.Error.cancelTransaction
        }
        guard case .rolledBack = result else { return XCTFail("expected rollback") }
        _ = try? await unawaitedTask.value?.value

        // The inherited-context write joins the transaction and rolls back with it
        try await Task.sleep(nanoseconds: 200_000_000)
        let joined = try await EdgeConcItem.read(from: db, id: 60)
        XCTAssertNil(joined, "a Task {} spawned in a transaction body inherits the transaction; its write should roll back with it")

        let rolledBack = try await EdgeConcItem.read(from: db, id: 50)
        XCTAssertNil(rolledBack, "transaction's own write must be rolled back")
        await db.close()
    }

    // Same scenario, but with `Task.detached` (which does NOT inherit the
    // transaction's TaskLocal): the barrier must defer the write past the
    // rollback, and it must land.
    func testDetachedTaskWriteDuringCancelledTransactionLands() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await EdgeConcItem(id: 1, title: "setup", num: 0).write(to: db)

        let detachedTask = Blackbird.Locked<Task<Void, Swift.Error>?>(nil)
        let result = try await db.cancellableTransaction { core in
            try EdgeConcItem(id: 51, title: "inside transaction", num: 0).write(to: core)

            detachedTask.value = Task.detached {
                try await EdgeConcItem(id: 61, title: "detached write", num: 0).write(to: db)
            }

            try await Task.sleep(nanoseconds: 300_000_000)
            throw Blackbird.Error.cancelTransaction
        }
        guard case .rolledBack = result else { return XCTFail("expected rollback") }
        try await detachedTask.value?.value

        var landed: EdgeConcItem? = nil
        for _ in 0..<20 {
            landed = try await EdgeConcItem.read(from: db, id: 61)
            if landed != nil { break }
            try await Task.sleep(nanoseconds: 100_000_000)
        }
        XCTAssertNotNil(landed, "detached write must be deferred past the rollback and land")

        let rolledBack = try await EdgeConcItem.read(from: db, id: 51)
        XCTAssertNil(rolledBack)
        await db.close()
    }

    // 100 sequential transactions must all commit, with an exact final count.
    func testSequentialTransactionsLoop() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)

        for i in 1...100 {
            try await db.transaction { core in
                try EdgeConcItem(id: Int64(i), title: "loop \(i)", num: i).write(to: core)
            }
        }

        let count = try await EdgeConcItem.count(in: db, matching: .all)
        XCTAssertEqual(count, 100)
        await db.close()
    }

    // MARK: - Barrier / gate interleaving

    // While an async transaction sleeps mid-body: a plain read, a structured
    // update, and a raw db.query must all complete after the transaction ends
    // (no deadlock), and none of their effects may be undone by its rollback.
    func testGatedOperationsDuringSleepingTransactionSurviveItsRollback() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await EdgeConcItem(id: 1, title: "setup", num: 0).write(to: db)

        let transactionTask = Task {
            try await db.cancellableTransaction { core in
                try EdgeConcItem(id: 100, title: "inside transaction", num: 0).write(to: core)
                try await Task.sleep(nanoseconds: 500_000_000)
                throw Blackbird.Error.cancelTransaction
            }
        }

        try await Task.sleep(nanoseconds: 150_000_000) // let the transaction start and suspend

        let readTask = Task { try await EdgeConcItem.read(from: db, id: 1) }
        let updateTask = Task { try await EdgeConcItem.update(in: db, set: [ \.$title : "updated" ], matching: \.$id == 1) }
        let rawTask = Task { try await db.query("INSERT INTO EdgeConcItem (id, title, num) VALUES (?, ?, ?)", 200, "raw insert", 0) }

        let transactionResult = try await transactionTask.value
        guard case .rolledBack = transactionResult else { return XCTFail("expected rollback") }

        // All gated operations must complete without deadlock
        let readValue = try await readTask.value
        try await updateTask.value
        _ = try await rawTask.value
        XCTAssertNotNil(readValue, "gated read must complete and find the pre-existing row")

        let ids = Set(try await EdgeConcItem.read(from: db, matching: .all).map(\.id))
        XCTAssertFalse(ids.contains(100), "rolled-back transaction write leaked")
        let row1AfterAll = try await EdgeConcItem.read(from: db, id: 1)
        let row200AfterAll = try await EdgeConcItem.read(from: db, id: 200)
        XCTAssertEqual(row1AfterAll?.title, "updated", "structured update was lost to the transaction rollback")
        XCTAssertEqual(row200AfterAll?.title, "raw insert", "raw insert was lost to the transaction rollback")
        await db.close()
    }

    // Two overlapping async transactions from different tasks must be strictly
    // serialized: a read-modify-write counter incremented in both (with a
    // deliberate mid-body suspension) must end at exactly 2.
    func testOverlappingAsyncTransactionsAreSerialized() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await EdgeConcCounter(id: 1, value: 0).write(to: db)

        let increment: @Sendable () async throws -> Void = {
            try await db.transaction { core in
                var counter = try EdgeConcCounter.read(from: core, id: 1)!
                try await Task.sleep(nanoseconds: 150_000_000) // suspend mid read-modify-write
                counter.value += 1
                try counter.write(to: core)
            }
        }

        let first = Task { try await increment() }
        try await Task.sleep(nanoseconds: 50_000_000) // ensure overlap
        let second = Task { try await increment() }

        try await first.value
        try await second.value

        let final = try await EdgeConcCounter.read(from: db, id: 1)
        XCTAssertEqual(final?.value, 2, "overlapping transactions interleaved a read-modify-write")
        await db.close()
    }

    // Transactions inside task-group children contending with plain writes from
    // other children: exact final row count, no losses, no deadlock.
    func testTransactionsInTaskGroupContendingWithPlainWrites() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await EdgeConcItem(id: 500, title: "setup", num: 0).write(to: db)

        try await withThrowingTaskGroup(of: Void.self) { group in
            for i in 0..<10 {
                group.addTask {
                    try await db.transaction { core in
                        try EdgeConcItem(id: Int64(1000 + i * 2), title: "tx \(i) a", num: i).write(to: core)
                        try EdgeConcItem(id: Int64(1001 + i * 2), title: "tx \(i) b", num: i).write(to: core)
                    }
                }
            }
            for i in 0..<20 {
                group.addTask {
                    try await EdgeConcItem(id: Int64(i), title: "plain \(i)", num: i).write(to: db)
                }
            }
            try await group.waitForAll()
        }

        let count = try await EdgeConcItem.count(in: db, matching: .all)
        XCTAssertEqual(count, 41) // 1 setup + 20 transaction writes + 20 plain writes
        await db.close()
    }

    // MARK: - Cache

    // A cacheLimit of 1 forces evictions on nearly every access: reads must
    // still always return correct values.
    func testCacheLimitOneEvictions() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)

        try await EdgeConcTinyCacheItem(id: 1, title: "one").write(to: db)
        try await EdgeConcTinyCacheItem(id: 2, title: "two").write(to: db)
        try await EdgeConcTinyCacheItem(id: 3, title: "three").write(to: db)

        let read1 = try await EdgeConcTinyCacheItem.read(from: db, id: 1)
        let read2 = try await EdgeConcTinyCacheItem.read(from: db, id: 2)
        let read3 = try await EdgeConcTinyCacheItem.read(from: db, id: 3)
        XCTAssertEqual(read1?.title, "one")
        XCTAssertEqual(read2?.title, "two")
        XCTAssertEqual(read3?.title, "three")

        // Update one row, then read all again in reverse order
        try await EdgeConcTinyCacheItem(id: 2, title: "two updated").write(to: db)
        let reread3 = try await EdgeConcTinyCacheItem.read(from: db, id: 3)
        let reread2 = try await EdgeConcTinyCacheItem.read(from: db, id: 2)
        let reread1 = try await EdgeConcTinyCacheItem.read(from: db, id: 1)
        XCTAssertEqual(reread3?.title, "three")
        XCTAssertEqual(reread2?.title, "two updated")
        XCTAssertEqual(reread1?.title, "one")
        await db.close()
    }

    // The instance cache must return value-equal instances for repeated reads,
    // and a raw-SQL UPDATE (bypassing the model layer) must invalidate it via
    // the update hook — for both cached and uncached models.
    func testRawSQLUpdateInvalidatesInstanceCache() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)

        // Cached model
        try await EdgeConcCachedItem(id: 1, title: "original").write(to: db)
        let firstRead = try await EdgeConcCachedItem.read(from: db, id: 1)
        let secondRead = try await EdgeConcCachedItem.read(from: db, id: 1)
        XCTAssertEqual(firstRead, secondRead, "cached reads must be value-equal")
        XCTAssertEqual(firstRead?.title, "original")

        try await db.query("UPDATE EdgeConcCachedItem SET title = ? WHERE id = ?", "updated raw", 1)
        let afterRawCached = try await EdgeConcCachedItem.read(from: db, id: 1)
        XCTAssertEqual(afterRawCached?.title, "updated raw", "raw-SQL UPDATE not visible through cached model read (update-hook invalidation failed)")

        // Uncached model
        try await EdgeConcItem(id: 1, title: "original", num: 0).write(to: db)
        try await db.query("UPDATE EdgeConcItem SET title = ? WHERE id = ?", "updated raw", 1)
        let afterRawUncached = try await EdgeConcItem.read(from: db, id: 1)
        XCTAssertEqual(afterRawUncached?.title, "updated raw")
        await db.close()
    }

    // Cache + transactions: a committed transaction's update must be readable
    // afterward; a rolled-back transaction's update must not be.
    func testCacheCoherenceAcrossCommitAndRollback() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)

        try await EdgeConcCachedItem(id: 1, title: "v1").write(to: db)
        let warm = try await EdgeConcCachedItem.read(from: db, id: 1) // warm the cache
        XCTAssertEqual(warm?.title, "v1")

        try await db.transaction { core in
            var row = try EdgeConcCachedItem.read(from: core, id: 1)!
            row.title = "v2"
            try row.write(to: core)
        }
        let afterCommit = try await EdgeConcCachedItem.read(from: db, id: 1)
        XCTAssertEqual(afterCommit?.title, "v2", "committed transaction update not visible through cache")

        let result = try await db.cancellableTransaction { core in
            var row = try EdgeConcCachedItem.read(from: core, id: 1)!
            row.title = "v3"
            try row.write(to: core)
            throw Blackbird.Error.cancelTransaction
        }
        guard case .rolledBack = result else { return XCTFail("expected rollback") }

        let afterRollback = try await EdgeConcCachedItem.read(from: db, id: 1)
        XCTAssertEqual(afterRollback?.title, "v2", "rolled-back transaction update leaked through the cache")
        await db.close()
    }

    // Concurrent cached reads and writes of the same row must not crash or
    // corrupt: after a final serialized write, the read must match it.
    func testConcurrentCachedReadsAndWritesSameRow() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await EdgeConcCachedItem(id: 1, title: "start").write(to: db)

        try await withThrowingTaskGroup(of: Void.self) { group in
            for i in 0..<50 {
                group.addTask {
                    if i % 2 == 0 {
                        try await EdgeConcCachedItem(id: 1, title: "writer \(i)").write(to: db)
                    } else {
                        let read = try await EdgeConcCachedItem.read(from: db, id: 1)
                        XCTAssertNotNil(read)
                    }
                }
            }
            try await group.waitForAll()
        }

        try await EdgeConcCachedItem(id: 1, title: "final").write(to: db)
        let final = try await EdgeConcCachedItem.read(from: db, id: 1)
        XCTAssertEqual(final?.title, "final")
        await db.close()
    }

    // While an async transaction holds uncommitted changes to a cached row, a
    // reader outside the transaction must not see the uncommitted value.
    //
    // SUSPECTED BUG (fails deterministically): `write(to: core)` calls
    // `_saveCachedInstance` immediately (BlackbirdModel.swift, end of
    // `write(to core:)`), even inside an open transaction, and cached reads
    // check `_cachedInstance` BEFORE `performGated` (BlackbirdModel.swift,
    // `read(from database:id:)`), so an outside reader gets the uncommitted
    // "dirty" value from the cache without ever waiting on the barrier —
    // a dirty read of data that is subsequently rolled back.
    func testCachedReadDuringOpenTransactionDoesNotReturnUncommittedData() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await EdgeConcCachedItem(id: 1, title: "clean").write(to: db)
        let warm = try await EdgeConcCachedItem.read(from: db, id: 1)
        XCTAssertEqual(warm?.title, "clean")

        let midTransactionReadTitle = Blackbird.Locked<String?>(nil)
        let readTask = Blackbird.Locked<Task<Void, Swift.Error>?>(nil)

        let result = try await db.cancellableTransaction { core in
            var row = try EdgeConcCachedItem.read(from: core, id: 1)!
            row.title = "dirty"
            try row.write(to: core)

            // Outside reader (detached: no inherited transaction TaskLocal)
            readTask.value = Task.detached {
                let outside = try await EdgeConcCachedItem.read(from: db, id: 1)
                midTransactionReadTitle.value = outside?.title
            }

            try await Task.sleep(nanoseconds: 400_000_000)
            throw Blackbird.Error.cancelTransaction
        }
        guard case .rolledBack = result else { return XCTFail("expected rollback") }
        try await readTask.value?.value

        let seenTitle = midTransactionReadTitle.value
        XCTAssertNotEqual(seenTitle, "dirty", "reader outside the transaction saw uncommitted (and later rolled-back) data via the instance cache")

        let after = try await EdgeConcCachedItem.read(from: db, id: 1)
        XCTAssertEqual(after?.title, "clean")
        await db.close()
    }

    // MARK: - Change reporting

    // One transaction touching three tables: each table's publisher must fire
    // exactly once, after commit — never mid-transaction.
    func testTransactionTouchingThreeTablesPublishesOncePerTable() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)

        // Resolve schemas outside the observed window, then drain pending notifications
        try await EdgeConcTableA(id: 0, v: "setup").write(to: db)
        try await EdgeConcTableB(id: 0, v: "setup").write(to: db)
        try await EdgeConcTableC(id: 0, v: "setup").write(to: db)
        try await Task.sleep(nanoseconds: 300_000_000)

        let aCount = Blackbird.Locked(0)
        let bCount = Blackbird.Locked(0)
        let cCount = Blackbird.Locked(0)
        let aExpectation = expectation(description: "A published")
        let bExpectation = expectation(description: "B published")
        let cExpectation = expectation(description: "C published")
        aExpectation.assertForOverFulfill = false
        bExpectation.assertForOverFulfill = false
        cExpectation.assertForOverFulfill = false

        let aSub = db.changePublisher(for: EdgeConcTableA.tableName).sink { _ in aCount.withLock { $0 += 1 }; aExpectation.fulfill() }
        let bSub = db.changePublisher(for: EdgeConcTableB.tableName).sink { _ in bCount.withLock { $0 += 1 }; bExpectation.fulfill() }
        let cSub = db.changePublisher(for: EdgeConcTableC.tableName).sink { _ in cCount.withLock { $0 += 1 }; cExpectation.fulfill() }
        defer {
            aSub.cancel()
            bSub.cancel()
            cSub.cancel()
        }

        let midTransactionCounts = Blackbird.Locked<[Int]>([])
        try await db.transaction { core in
            try EdgeConcTableA(id: 1, v: "in transaction").write(to: core)
            try EdgeConcTableB(id: 1, v: "in transaction").write(to: core)
            try EdgeConcTableC(id: 1, v: "in transaction").write(to: core)

            // Give any (incorrect) mid-transaction notification time to deliver
            try await Task.sleep(nanoseconds: 300_000_000)
            midTransactionCounts.value = [aCount.value, bCount.value, cCount.value]
        }

        await fulfillment(of: [aExpectation, bExpectation, cExpectation], timeout: 5)
        try await Task.sleep(nanoseconds: 300_000_000) // catch any duplicate sends

        XCTAssertEqual(midTransactionCounts.value, [0, 0, 0], "change published mid-transaction")
        let finalCounts = [aCount.value, bCount.value, cCount.value]
        XCTAssertEqual(finalCounts, [1, 1, 1], "each table's publisher must fire exactly once per committed transaction")
        await db.close()
    }

    // changePublisher(primaryKey:): a write to the matching key fires; a write
    // to a different key must not.
    func testChangePublisherPrimaryKeyFilter() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await EdgeConcItem(id: 1, title: "setup 1", num: 0).write(to: db)
        try await EdgeConcItem(id: 2, title: "setup 2", num: 0).write(to: db)
        try await Task.sleep(nanoseconds: 300_000_000) // drain pending notifications

        let matchCount = Blackbird.Locked(0)
        let matchExpectation = expectation(description: "matching key published")
        matchExpectation.assertForOverFulfill = false
        let subscription = EdgeConcItem.changePublisher(in: db, primaryKey: Int64(1)).sink { _ in
            matchCount.withLock { $0 += 1 }
            matchExpectation.fulfill()
        }
        defer { subscription.cancel() }

        // Non-matching key: no event within 400ms
        try await EdgeConcItem(id: 2, title: "non-matching write", num: 1).write(to: db)
        try await Task.sleep(nanoseconds: 400_000_000)
        let countAfterNonMatching = matchCount.value
        XCTAssertEqual(countAfterNonMatching, 0, "publisher fired for a non-matching primary key")

        // Matching key: event
        try await EdgeConcItem(id: 1, title: "matching write", num: 1).write(to: db)
        await fulfillment(of: [matchExpectation], timeout: 5)
        let finalCount = matchCount.value
        XCTAssertGreaterThanOrEqual(finalCount, 1)
        await db.close()
    }

    // ignoredColumns publisher variant: changing ONLY the ignored column must
    // not fire; changing another column must.
    func testIgnoredColumnsPublisherFiltering() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await EdgeConcIgnorable(id: 1, watched: "w1", ignored: "i1").write(to: db)
        try await Task.sleep(nanoseconds: 300_000_000) // drain pending notifications

        let eventCount = Blackbird.Locked(0)
        let watchedExpectation = expectation(description: "non-ignored column change published")
        watchedExpectation.assertForOverFulfill = false
        let subscription = EdgeConcIgnorable.changePublisher(in: db, ignoredColumns: [ \.$ignored ]).sink { _ in
            eventCount.withLock { $0 += 1 }
            watchedExpectation.fulfill()
        }
        defer { subscription.cancel() }

        // Change only the ignored column
        var row = try await EdgeConcIgnorable.read(from: db, id: 1)!
        row.ignored = "i2"
        try await row.write(to: db)
        try await Task.sleep(nanoseconds: 400_000_000)
        let countAfterIgnoredChange = eventCount.value
        XCTAssertEqual(countAfterIgnoredChange, 0, "publisher fired for a change to only the ignored column")

        // Change a watched column
        var row2 = try await EdgeConcIgnorable.read(from: db, id: 1)!
        row2.watched = "w2"
        try await row2.write(to: db)
        await fulfillment(of: [watchedExpectation], timeout: 5)
        let finalCount = eventCount.value
        XCTAssertGreaterThanOrEqual(finalCount, 1)
        await db.close()
    }

    // NOTE: `legacyChangeNotifications` does not exist in this codebase (verified
    // by grep across Sources/Blackbird), so no test for it.

    // Rapid-fire sequential writes to one row: the publisher must deliver
    // between 1 and 200 events (coalescing allowed), and once events go
    // quiescent, the last write's value must be readable.
    func testRapidFireWritesPublisherCoalescing() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await EdgeConcItem(id: 1, title: "setup", num: -1).write(to: db)
        try await Task.sleep(nanoseconds: 300_000_000) // drain pending notifications

        let eventCount = Blackbird.Locked(0)
        let subscription = db.changePublisher(for: EdgeConcItem.tableName).sink { _ in
            eventCount.withLock { $0 += 1 }
        }
        defer { subscription.cancel() }

        for i in 0..<200 {
            try await EdgeConcItem(id: 1, title: "t\(i)", num: i).write(to: db)
        }

        // Wait for event quiescence: no new events across a polling interval
        var lastCount = -1
        for _ in 0..<20 {
            try await Task.sleep(nanoseconds: 150_000_000)
            let current = eventCount.value
            if current == lastCount { break }
            lastCount = current
        }

        let events = eventCount.value
        XCTAssertGreaterThanOrEqual(events, 1, "no change events delivered for 200 writes")
        XCTAssertLessThanOrEqual(events, 200, "more events than writes")

        let final = try await EdgeConcItem.read(from: db, id: 1)
        XCTAssertEqual(final?.title, "t199", "final write's value not readable after the last event")
        await db.close()
    }

    // MARK: - Primitive stress

    // Blackbird.Semaphore with value 1: 20 tasks × 25 wait/signal loops must
    // all complete, with mutual exclusion never violated.
    func testSemaphoreStressWaitSignal() async throws {
        let semaphore = Blackbird.Semaphore(value: 1)
        let completed = Blackbird.Locked(0)
        let insideCritical = Blackbird.Locked(0)
        let maxInsideCritical = Blackbird.Locked(0)

        await withTaskGroup(of: Void.self) { group in
            for _ in 0..<20 {
                group.addTask {
                    for _ in 0..<25 {
                        await semaphore.wait()
                        let depth = insideCritical.withLock { $0 += 1; return $0 }
                        maxInsideCritical.withLock { if depth > $0 { $0 = depth } }
                        await Task.yield() // force interleaving inside the critical section
                        completed.withLock { $0 += 1 }
                        insideCritical.withLock { $0 -= 1 }
                        semaphore.signal()
                    }
                }
            }
            await group.waitForAll()
        }

        XCTAssertEqual(completed.value, 500, "some semaphore waiters never completed (lost wakeup?)")
        XCTAssertEqual(maxInsideCritical.value, 1, "semaphore admitted more than one task at once")
    }

    // Blackbird.Locked: concurrent increments from 100 tasks must total exactly 100.
    func testLockedConcurrentIncrements() async throws {
        let counter = Blackbird.Locked(0)

        await withTaskGroup(of: Void.self) { group in
            for _ in 0..<100 {
                group.addTask { counter.withLock { $0 += 1 } }
            }
            await group.waitForAll()
        }

        XCTAssertEqual(counter.value, 100)
    }
}
