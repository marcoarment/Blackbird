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
//  BlackbirdRegressionTests.swift
//
//  Regression tests for specific bugs: transaction/savepoint integrity, the
//  async-transaction barrier, changed-column tracking across instance copies,
//  cache coherence on failed writes, change-report merging, structured-query
//  expression compilation, decoder range safety, and FTS index integrity.
//

import XCTest
import Combine
import SQLite3
@testable import Blackbird

// MARK: - Models

struct RegressionCachedUniqueModel: BlackbirdModel {
    static let cacheLimit: Int = 100
    static let uniqueIndexes: [[BlackbirdColumnKeyPath]] = [
        [ \.$slug ],
    ]

    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var slug: String
    @BlackbirdColumn var title: String
}

struct RegressionNarrowIntModel: BlackbirdModel {
    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var tiny: Int8
}

struct RegressionFTSQuoteModel: BlackbirdModel {
    static let fullTextSearchableColumns: FullTextIndex = [
        \.$title : .text,
    ]

    @BlackbirdColumn var id: Int
    @BlackbirdColumn var title: String
}

struct RegressionFTSRebuildInitial: BlackbirdModel {
    static let tableName = "RegressionFTSRebuild"
    static let primaryKey: [BlackbirdColumnKeyPath] = [ \.$key ]
    static let fullTextSearchableColumns: FullTextIndex = [
        \.$title : .text,
    ]

    var id: String { key }

    @BlackbirdColumn var key: String
    @BlackbirdColumn var title: String
    @BlackbirdColumn var flags: Int
}

struct RegressionFTSRebuildChanged: BlackbirdModel {
    static let tableName = "RegressionFTSRebuild"
    static let primaryKey: [BlackbirdColumnKeyPath] = [ \.$key ]
    static let fullTextSearchableColumns: FullTextIndex = [
        \.$title : .text,
    ]

    var id: String { key }

    @BlackbirdColumn var key: String
    @BlackbirdColumn var title: String
    @BlackbirdColumn var flags: String // changed type from Int: forces a full table rebuild
}

#if canImport(CloudKit)
struct RegressionSkybridgeModel: BlackbirdModel, BlackbirdSkybridgeSyncable {
    static let primaryKey: [BlackbirdColumnKeyPath] = [ \.$key ]
    static let skybridgeExcludedColumns: [BlackbirdColumnKeyPath] = [ \.$localOnly ]

    var id: String { key }

    @BlackbirdColumn var key: String
    @BlackbirdColumn var title: String?
    @BlackbirdColumn var dueDate: Date?
    @BlackbirdColumn var localOnly: String?
    @BlackbirdColumn var skybridgeMetadata: Data?
}
#endif

// MARK: - Tests

final class BlackbirdRegressionTests: XCTestCase, @unchecked Sendable {
    enum Error: Swift.Error {
        case testError
    }

    var sqliteFilename = ""

    override func setUpWithError() throws {
        let dir = FileManager.default.temporaryDirectory.path
        sqliteFilename = "\(dir)/test\(Int64.random(in: 0..<Int64.max)).sqlite"
    }

    override func tearDownWithError() throws {
        if sqliteFilename != "", sqliteFilename != ":memory:", FileManager.default.fileExists(atPath: sqliteFilename) {
            for path in Blackbird.Database.allFilePaths(for: sqliteFilename) {
                try? FileManager.default.removeItem(atPath: path)
            }
        }
    }

    // MARK: Transactions and savepoints

    // A rolled-back transaction must not leave the connection inside an open
    // transaction: all writes after it must survive close and reopen.
    func testWritesPersistAfterRolledBackTransaction() async throws {
        var db = try Blackbird.Database(path: sqliteFilename)

        let result = try await db.cancellableTransaction { core in
            try TestModel(id: 1, title: "rolled back", url: TestData.randomURL).write(to: core)
            throw Blackbird.Error.cancelTransaction
        }
        guard case .rolledBack = result else { return XCTFail("expected rollback") }

        try await db.transaction { core in
            try TestModel(id: 2, title: "committed transaction", url: TestData.randomURL).write(to: core)
        }
        try await TestModel(id: 3, title: "plain write", url: TestData.randomURL).write(to: db)

        await db.close()
        db = try Blackbird.Database(path: sqliteFilename)

        let all = try await TestModel.read(from: db, matching: .all)
        XCTAssertEqual(Set(all.map(\.id)), [2, 3])
        await db.close()
    }

    // Same, but for a transaction that fails with an arbitrary thrown error.
    func testWritesPersistAfterFailedTransaction() async throws {
        var db = try Blackbird.Database(path: sqliteFilename)

        do {
            try await db.transaction { core in
                try TestModel(id: 1, title: "failed", url: TestData.randomURL).write(to: core)
                throw Error.testError
            }
            XCTFail("expected throw")
        } catch Error.testError { } // expected

        try await TestModel(id: 2, title: "after failure", url: TestData.randomURL).write(to: db)

        await db.close()
        db = try Blackbird.Database(path: sqliteFilename)

        let all = try await TestModel.read(from: db, matching: .all)
        XCTAssertEqual(all.map(\.id), [2])
        await db.close()
    }

    // Cancelling inside plain transaction() must throw, not crash.
    func testCancelInsidePlainTransactionThrows() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        defer { Task { await db.close() } }

        do {
            try await db.transaction { core in
                try TestModel(id: 1, title: "x", url: TestData.randomURL).write(to: core)
                throw Blackbird.Error.cancelTransaction
            }
            XCTFail("expected throw")
        } catch Blackbird.Error.cancelTransaction { } // expected

        let all = try await TestModel.read(from: db, matching: .all)
        XCTAssert(all.isEmpty)
    }

    // A transaction opened from within another transaction's action must nest
    // (inner savepoint) instead of deadlocking on the transaction semaphore.
    func testNestedAsyncTransactions() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)

        try await db.transaction { core in
            try TestModel(id: 1, title: "outer", url: TestData.randomURL).write(to: core)

            let inner = try await db.cancellableTransaction { core in
                try TestModel(id: 2, title: "inner rolled back", url: TestData.randomURL).write(to: core)
                throw Blackbird.Error.cancelTransaction
            }
            guard case .rolledBack = inner else { return XCTFail("expected inner rollback") }

            try await db.transaction { core in
                try TestModel(id: 3, title: "inner committed", url: TestData.randomURL).write(to: core)
            }
        }

        let all = try await TestModel.read(from: db, matching: .all)
        XCTAssertEqual(Set(all.map(\.id)), [1, 3])
        await db.close()
    }

    // A concurrent plain write during a suspended async transaction must not
    // land inside its savepoint: when the transaction rolls back, the
    // concurrent write must survive.
    func testConcurrentWriteDuringAsyncTransactionSurvivesRollback() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await TestModel(id: 1, title: "setup", url: TestData.randomURL).write(to: db) // resolve schema

        let transactionTask = Task {
            try await db.cancellableTransaction { core in
                try TestModel(id: 100, title: "inside transaction", url: TestData.randomURL).write(to: core)
                try await Task.sleep(nanoseconds: 800_000_000)
                throw Blackbird.Error.cancelTransaction
            }
        }

        try await Task.sleep(nanoseconds: 200_000_000)
        let concurrentWriteTask = Task {
            try await TestModel(id: 200, title: "concurrent", url: TestData.randomURL).write(to: db)
        }

        let result = try await transactionTask.value
        guard case .rolledBack = result else { return XCTFail("expected rollback") }
        try await concurrentWriteTask.value

        let ids = Set(try await TestModel.read(from: db, matching: .all).map(\.id))
        XCTAssertFalse(ids.contains(100), "rolled-back transaction write leaked")
        XCTAssert(ids.contains(200), "concurrent write was rolled back with the transaction")
        await db.close()
    }

    // Many concurrent transactions must all complete (semaphore lost-wakeup regression).
    func testConcurrentTransactionStress() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await TestModel(id: 10_000, title: "setup", url: TestData.randomURL).write(to: db)

        try await withThrowingTaskGroup(of: Void.self) { group in
            for i in 0..<50 {
                group.addTask {
                    try await db.transaction { core in
                        try TestModel(id: Int64(i), title: "stress \(i)", url: TestData.randomURL).write(to: core)
                    }
                }
            }
            try await group.waitForAll()
        }

        let count = try await TestModel.count(in: db, matching: .all)
        XCTAssertEqual(count, 51)
        await db.close()
    }

    // Concurrent modify() calls on the same row must not lose updates.
    func testConcurrentModifyAtomicity() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await TestModelForUpdateExpressions(id: 1, i: 0, d: 0).write(to: db)

        try await withThrowingTaskGroup(of: Void.self) { group in
            for _ in 0..<100 {
                group.addTask {
                    try await TestModelForUpdateExpressions.modify(in: db, primaryKey: 1) { _, instance in
                        instance.i += 1
                    }
                }
            }
            try await group.waitForAll()
        }

        let final = try await TestModelForUpdateExpressions.read(from: db, id: 1)
        XCTAssertEqual(final?.i, 100)
        await db.close()
    }

    // A write colliding with another connection's write lock must wait
    // (busy_timeout) instead of failing instantly with SQLITE_BUSY.
    func testBusyTimeoutWaitsForOtherConnections() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await TestModel(id: 1, title: "setup", url: TestData.randomURL).write(to: db)

        struct RawHandle: @unchecked Sendable { let pointer: OpaquePointer }
        var rawPointer: OpaquePointer? = nil
        XCTAssertEqual(sqlite3_open(sqliteFilename, &rawPointer), SQLITE_OK)
        let raw = RawHandle(pointer: rawPointer!)
        XCTAssertEqual(sqlite3_exec(raw.pointer, "BEGIN IMMEDIATE", nil, nil, nil), SQLITE_OK)

        let releaseTask = Task.detached {
            try await Task.sleep(nanoseconds: 1_000_000_000)
            sqlite3_exec(raw.pointer, "COMMIT", nil, nil, nil)
        }

        // Without a busy timeout this throws SQLITE_BUSY immediately.
        try await TestModel(id: 2, title: "waited", url: TestData.randomURL).write(to: db)

        _ = await releaseTask.result
        sqlite3_close(raw.pointer)

        let waited = try await TestModel.read(from: db, id: 2)
        XCTAssertNotNil(waited)
        await db.close()
    }

    // MARK: Schema resolution

    // Schema resolution inside a rolled-back transaction must not be memoized:
    // the table must still be creatable and usable afterward.
    func testSchemaResolutionSurvivesRolledBackTransaction() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)

        let result = try await db.cancellableTransaction { core in
            try TestModel(id: 1, title: "first touch", url: TestData.randomURL).write(to: core)
            throw Blackbird.Error.cancelTransaction
        }
        guard case .rolledBack = result else { return XCTFail("expected rollback") }

        try await TestModel(id: 2, title: "after rollback", url: TestData.randomURL).write(to: db)
        let all = try await TestModel.read(from: db, matching: .all)
        XCTAssertEqual(all.map(\.id), [2])
        await db.close()
    }

    // MARK: Changed-column tracking

    // A copy's write must persist its own changes even if the original
    // instance was written (clearing the shared flags) in between.
    func testSiblingCopyWritePersists() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)

        try await TestModel(id: 1, title: "Original", url: TestData.randomURL).write(to: db)
        let a = try await TestModel.read(from: db, id: 1)!
        var b = a
        b.title = "Updated by copy"

        try await a.write(to: db) // clears the flags shared with b
        try await b.write(to: db) // must still persist b's title

        let final = try await TestModel.read(from: db, id: 1)
        XCTAssertEqual(final?.title, "Updated by copy")
        await db.close()
    }

    // Same scenario through the instance cache, which hands out shared copies aggressively.
    func testSiblingCopyWritePersistsWithCache() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)

        try await TestModelWithCache(id: 1, title: "Original", url: TestData.randomURL).write(to: db)
        let a = try await TestModelWithCache.read(from: db, id: 1)!
        var b = try await TestModelWithCache.read(from: db, id: 1)!
        b.title = "Updated by copy"

        try await a.write(to: db)
        try await b.write(to: db)

        let final = try await TestModelWithCache.read(from: db, id: 1)
        XCTAssertEqual(final?.title, "Updated by copy")
        await db.close()
    }

    // MARK: Cache coherence

    // A write that fails (unique-index conflict) must neither cache the
    // unsaved instance nor leave stale data readable.
    func testFailedWriteDoesNotPoisonCache() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)

        try await RegressionCachedUniqueModel(id: 1, slug: "same", title: "first").write(to: db)
        let warmed = try await RegressionCachedUniqueModel.read(from: db, id: 1) // warm the cache
        XCTAssertNotNil(warmed)

        do {
            try await RegressionCachedUniqueModel(id: 2, slug: "same", title: "conflicting").write(to: db)
            XCTFail("expected unique-constraint failure")
        } catch Blackbird.Database.Error.uniqueConstraintFailed { } // expected

        let phantom = try await RegressionCachedUniqueModel.read(from: db, id: 2)
        XCTAssertNil(phantom, "failed write left a phantom instance in the cache")

        let original = try await RegressionCachedUniqueModel.read(from: db, id: 1)
        XCTAssertEqual(original?.title, "first")
        await db.close()
    }

    // A rolled-back transaction must invalidate cached instances of rows it touched.
    func testRolledBackTransactionInvalidatesCache() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)

        try await TestModelWithCache(id: 1, title: "Original", url: TestData.randomURL).write(to: db)
        let before = try await TestModelWithCache.read(from: db, id: 1)
        XCTAssertEqual(before?.title, "Original")

        let result = try await db.cancellableTransaction { core in
            var t = try TestModelWithCache.read(from: core, id: 1)!
            t.title = "Uncommitted"
            try t.write(to: core)
            throw Blackbird.Error.cancelTransaction
        }
        guard case .rolledBack = result else { return XCTFail("expected rollback") }

        let after = try await TestModelWithCache.read(from: db, id: 1)
        XCTAssertEqual(after?.title, "Original")
        await db.close()
    }

    // The structured-query cache must actually serve repeated queries.
    func testStructuredQueryCacheHits() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        for i in 0..<5 {
            try await TestModelWithCache(id: Int64(i), title: "row \(i)", url: TestData.randomURL).write(to: db)
        }

        await db.setArtificialQueryDelay(0.5)

        let start1 = Date()
        let first = try await TestModelWithCache.read(from: db, matching: .all)
        let duration1 = -start1.timeIntervalSinceNow

        let start2 = Date()
        let second = try await TestModelWithCache.read(from: db, matching: .all)
        let duration2 = -start2.timeIntervalSinceNow

        await db.setArtificialQueryDelay(nil)

        XCTAssertEqual(first.count, 5)
        XCTAssertEqual(second.count, 5)
        XCTAssert(duration1 >= 0.4, "first read should have hit SQLite (delayed)")
        XCTAssert(duration2 < 0.25, "second read should have come from the cache, took \(duration2)s")
        await db.close()
    }

    // MARK: Change reporting

    // A whole-table change following keyed changes in the same transaction
    // must not discard the earlier changes' column names.
    func testChangeMergePreservesColumnNames() async throws {
        let db = try Blackbird.Database.inMemoryDatabase()

        let received = Blackbird.Locked<Blackbird.Change?>(nil)
        let expectation = expectation(description: "change published")
        let subscription = db.changeReporter.changePublisher(for: "MergeTest").sink { change in
            received.value = change
            expectation.fulfill()
        }
        defer { subscription.cancel() }

        db.changeReporter.beginTransaction(999)
        db.changeReporter.reportChange(tableName: "MergeTest", primaryKeys: [[.integer(1)]], changedColumns: ["title"])
        db.changeReporter.reportChange(tableName: "MergeTest", primaryKeys: nil, changedColumns: ["archived"])
        db.changeReporter.endTransaction(999)

        await fulfillment(of: [expectation], timeout: 5)
        guard let change = received.value else { return XCTFail("no change received") }
        XCTAssert(change.hasColumnChanged("title"), "keyed change's column was dropped by the whole-table merge")
        XCTAssert(change.hasColumnChanged("archived"))
        XCTAssert(change.hasPrimaryKeyChanged(12345), "whole-table change should report all keys as changed")
        await db.close()
    }

    // Every subscriber must receive published changes, regardless of subscription order.
    func testAllSubscribersReceiveChanges() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)

        let firstExpectation = expectation(description: "first subscriber")
        let secondExpectation = expectation(description: "second subscriber")
        let firstSubscription = db.changePublisher(for: TestModel.tableName).sink { _ in firstExpectation.fulfill() }
        let secondSubscription = db.changePublisher(for: TestModel.tableName).sink { _ in secondExpectation.fulfill() }
        defer {
            firstSubscription.cancel()
            secondSubscription.cancel()
        }

        try await TestModel(id: 1, title: "notify", url: TestData.randomURL).write(to: db)

        await fulfillment(of: [firstExpectation, secondExpectation], timeout: 5)
        await db.close()
    }

    // Structured updates on an FTS-indexed table must report specific primary
    // keys, not a whole-table (nil-key) change: the FTS triggers' shadow-table
    // writes previously tripped the change-count sanity check.
    func testFTSTableUpdateReportsSpecificKeys() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        for i in 0..<5 {
            try await RegressionFTSQuoteModel(id: i, title: "row number \(i)").write(to: db)
        }

        let receivedKeys = Blackbird.Locked<Blackbird.PrimaryKeyValues??>(nil)
        let expectation = expectation(description: "change published")
        let subscription = RegressionFTSQuoteModel.changePublisher(in: db).sink { change in
            receivedKeys.value = change.changedPrimaryKeys
            expectation.fulfill()
        }
        defer { subscription.cancel() }

        try await RegressionFTSQuoteModel.update(in: db, set: [ \.$title : "updated title" ], matching: \.$id == 3)

        await fulfillment(of: [expectation], timeout: 5)
        guard let keys = receivedKeys.value else { return XCTFail("no change received") }
        XCTAssertNotNil(keys, "single-row update on FTS table reported a whole-table change")
        XCTAssertEqual(keys, [[.integer(3)]])
        await db.close()
    }

    // MARK: Structured-query expression compilation

    // .literal() must be parenthesized so OR inside it can't leak precedence.
    func testLiteralExpressionParenthesization() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await TestModelWithDescription(id: 1, url: nil, title: "other", description: "").write(to: db)
        try await TestModelWithDescription(id: 2, url: nil, title: "keep", description: "").write(to: db)
        try await TestModelWithDescription(id: 3, url: nil, title: "keep", description: "").write(to: db)

        let results = try await TestModelWithDescription.read(from: db, matching: .literal("id = ? OR id = ?", 1, 2) && \.$title == "keep")
        XCTAssertEqual(results.map(\.id), [2], "OR inside .literal leaked out of the AND")
        await db.close()
    }

    // Combining with .all: `x || .all` must match everything; `.all && .all` must not be a syntax error.
    func testCombiningWithMatchAll() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        for i in 1...3 {
            try await TestModelWithDescription(id: i, url: nil, title: "row \(i)", description: "").write(to: db)
        }

        let orAll = try await TestModelWithDescription.read(from: db, matching: \.$id == 1 || .all)
        XCTAssertEqual(orAll.count, 3, "x OR .all must match all rows")

        let andAll = try await TestModelWithDescription.read(from: db, matching: .all && .all)
        XCTAssertEqual(andAll.count, 3)

        let andExpr = try await TestModelWithDescription.read(from: db, matching: \.$id == 1 && .all)
        XCTAssertEqual(andExpr.map(\.id), [1])
        await db.close()
    }

    // An empty primary-key collection must be a no-op, not a SQL syntax error.
    func testUpdateWithEmptyPrimaryKeyCollection() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await TestModelForUpdateExpressions(id: 1, i: 1, d: 1).write(to: db)

        try await TestModelForUpdateExpressions.update(in: db, set: [ \.$i : 99 ], forPrimaryKeys: [Int64]())

        let unchanged = try await TestModelForUpdateExpressions.read(from: db, id: 1)
        XCTAssertEqual(unchanged?.i, 1)
        await db.close()
    }

    // MARK: Value and decoder safety

    // Out-of-range stored integers must throw a decoding error, not crash the process.
    func testOutOfRangeIntegerDecodeThrows() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await RegressionNarrowIntModel(id: 1, tiny: 7).write(to: db)
        try await db.query("UPDATE RegressionNarrowIntModel SET tiny = 300 WHERE id = 1")

        await AssertThrowsErrorAsync(_ = try await RegressionNarrowIntModel.read(from: db, id: 1))
        await db.close()
    }

    func testValueEdgeCases() throws {
        XCTAssertEqual(Blackbird.Value.integer(-1).boolValue, true) // SQLite truthiness: any nonzero integer
        XCTAssertEqual(Blackbird.Value.integer(0).boolValue, false)
        XCTAssertEqual(Blackbird.Value.double(-0.5).boolValue, true)

        XCTAssertNil(Blackbird.Value.double(.infinity).intValue)
        XCTAssertNil(Blackbird.Value.double(.nan).intValue)
        XCTAssertNil(Blackbird.Value.double(1e300).int64Value)
        XCTAssertEqual(Blackbird.Value.double(3.7).intValue, 3)
        XCTAssertEqual(Blackbird.Value.double(-3.7).intValue, -3)

        // Hashable consistency
        XCTAssertEqual(Blackbird.Value.integer(1).hashValue, Blackbird.Value.integer(1).hashValue)
        XCTAssertEqual(Blackbird.Value.text("a").hashValue, Blackbird.Value.text("a").hashValue)
        XCTAssertEqual(Blackbird.Value.data(Data([1, 2, 3])).hashValue, Blackbird.Value.data(Data([1, 2, 3])).hashValue)

        // Optional unwrapping through fromAny
        XCTAssertEqual(try Blackbird.Value.fromAny(Optional<String>.none), .null)
        XCTAssertEqual(try Blackbird.Value.fromAny(Optional<String>.some("x")), .text("x"))
    }

    // MARK: FTS

    // Quote characters in search queries must not produce FTS5 syntax errors,
    // and quoted phrases must actually constrain results.
    func testFTSQuoteHandling() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await RegressionFTSQuoteModel(id: 1, title: "the accidental tech podcast show").write(to: db)
        try await RegressionFTSQuoteModel(id: 2, title: "accidental technology podding").write(to: db)
        try await RegressionFTSQuoteModel(id: 3, title: "podcast about tech accidental things").write(to: db)
        try await RegressionFTSQuoteModel(id: 4, title: "use 5\" nails for this").write(to: db)

        // Unbalanced quote: must not throw "unterminated string"
        let nails = try await RegressionFTSQuoteModel.fullTextSearch(from: db, matching: .match("5\" nails"), options: .default)
        XCTAssertEqual(nails.count, 1)

        // Quoted phrase: must not match rows that merely contain both words
        let phrase = try await RegressionFTSQuoteModel.fullTextSearch(from: db, matching: .match("\"accidental tech\" podcast"), options: .default)
        let phraseIDs = Set(try await instanceIDs(of: phrase, in: db))
        XCTAssertEqual(phraseIDs, [1], "phrase constraint was not applied")
        await db.close()
    }

    private func instanceIDs(of results: [BlackbirdModelSearchResult<RegressionFTSQuoteModel>], in db: Blackbird.Database) async throws -> [Int] {
        var ids: [Int] = []
        for result in results {
            if let instance = try await result.instance(from: db) { ids.append(instance.id) }
        }
        return ids
    }

    // Updating an INTEGER primary key (a rowid alias) must keep the FTS index in sync.
    func testFTSPrimaryKeyUpdateKeepsIndexInSync() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await RegressionFTSQuoteModel(id: 3, title: "unique zebra sighting").write(to: db)

        try await RegressionFTSQuoteModel.update(in: db, set: [ \.$id : 300 ], matching: \.$id == 3)

        let results = try await RegressionFTSQuoteModel.fullTextSearch(from: db, matching: .match("zebra"), options: .default)
        XCTAssertEqual(results.count, 1)
        let instance = try await results.first!.instance(from: db)
        XCTAssertEqual(instance?.id, 300, "FTS index points at a stale rowid after primary-key update")
        await db.close()
    }

    // A full table rebuild (column type change) must preserve the FTS mapping:
    // search results must still resolve to the correct rows even with rowid
    // gaps, and the recreated triggers must index subsequent writes.
    func testTableRebuildPreservesFTSIntegrity() async throws {
        var db = try Blackbird.Database(path: sqliteFilename)
        let titles = ["alpha", "bravo", "charlie", "delta", "echo"]
        for (i, title) in titles.enumerated() {
            try await RegressionFTSRebuildInitial(key: "k\(i)", title: title, flags: i).write(to: db)
        }
        // Create rowid gaps
        try await RegressionFTSRebuildInitial.read(from: db, primaryKey: "k1")?.delete(from: db)
        try await RegressionFTSRebuildInitial.read(from: db, primaryKey: "k2")?.delete(from: db)
        await db.close()

        db = try Blackbird.Database(path: sqliteFilename)
        let resolution = try await RegressionFTSRebuildChanged.resolveSchema(in: db)
        XCTAssert(resolution.contains(.migratedTable))

        let echo = try await RegressionFTSRebuildChanged.fullTextSearch(from: db, matching: .match("echo"), options: .default)
        XCTAssertEqual(echo.count, 1)
        let echoInstance = try await echo.first!.instance(from: db)
        XCTAssertEqual(echoInstance?.key, "k4", "FTS result resolved to the wrong row after table rebuild")

        // Triggers must exist after the rebuild: new writes must be searchable
        try await RegressionFTSRebuildChanged(key: "k9", title: "zulu arrival", flags: "new").write(to: db)
        let zulu = try await RegressionFTSRebuildChanged.fullTextSearch(from: db, matching: .match("zulu"), options: .default)
        XCTAssertEqual(zulu.count, 1)
        let zuluInstance = try await zulu.first!.instance(from: db)
        XCTAssertEqual(zuluInstance?.key, "k9", "FTS triggers were lost in the table rebuild")
        await db.close()
    }

    // A database whose FTS triggers were created by an older version (before
    // primary-key columns were added to the update trigger) must detect the
    // outdated trigger SQL and rebuild at schema resolution.
    func testOutdatedTriggersForceRebuild() async throws {
        var db = try Blackbird.Database(path: sqliteFilename)
        try await RegressionFTSQuoteModel(id: 1, title: "golf hotel india").write(to: db)

        // Replace the update trigger with the old-format definition (no PK column in UPDATE OF)
        let tableName = RegressionFTSQuoteModel.tableName
        try await db.query("DROP TRIGGER `\(tableName)+FTSUpdate`")
        try await db.execute(
            """
            CREATE TRIGGER `\(tableName)+FTSUpdate` AFTER UPDATE OF `title` ON `\(tableName)` BEGIN
                INSERT INTO `\(tableName)+FTS`(`\(tableName)+FTS`,rowid,`title`) VALUES ('delete',OLD.rowid,OLD.`title`);
                INSERT INTO `\(tableName)+FTS`(rowid,`title`) VALUES (NEW.rowid,NEW.`title`);
            END
            """
        )
        await db.close()

        db = try Blackbird.Database(path: sqliteFilename)
        let resolution = try await RegressionFTSQuoteModel.resolveSchema(in: db)
        XCTAssert(resolution.contains(.migratedFullTextIndex), "outdated trigger SQL was not detected")

        // The upgraded trigger must now handle primary-key (rowid) updates
        try await RegressionFTSQuoteModel.update(in: db, set: [ \.$id : 100 ], matching: \.$id == 1)
        let results = try await RegressionFTSQuoteModel.fullTextSearch(from: db, matching: .match("hotel"), options: .default)
        XCTAssertEqual(results.count, 1)
        let instance = try await results.first!.instance(from: db)
        XCTAssertEqual(instance?.id, 100)
        await db.close()
    }

    // Asking for a snippet of a column that isn't full-text indexed must return nil, not crash.
    func testSnippetOfNonIndexedColumnReturnsNil() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await FTSModel(id: 1, title: "searchable tech title", url: TestData.randomURL, description: "body", keywords: "kw", category: 1).write(to: db)

        let results = try await FTSModel.fullTextSearch(from: db, matching: .match("tech"), options: .default)
        XCTAssertEqual(results.count, 1)
        XCTAssertNil(results.first!.snippet(\.$keywords)) // keywords is not FTS-indexed in FTSModel
        await db.close()
    }

    // MARK: Skybridge

#if canImport(CloudKit)
    // Opt-out column selection: everything syncs except the primary key,
    // skybridgeMetadata, and explicitly excluded columns.
    func testSkybridgeSyncedColumnNames() throws {
        let names = RegressionSkybridgeModel.skybridgeSyncedColumnNames()
        XCTAssertEqual(Set(names), ["title", "dueDate"])
    }

    func testSkybridgeMetadataRoundTrip() throws {
        // Empty metadata encodes as nil, so untouched rows stay clean
        XCTAssertNil(SkybridgeMetadata().encoded())

        var instance = RegressionSkybridgeModel(key: "k", title: nil, dueDate: nil, localOnly: nil, skybridgeMetadata: nil)
        XCTAssertNil(instance.skybridgeDecodedMetadata.ckRecordData)
        XCTAssertNil(instance.skybridgeDecodedMetadata.syncLastModifiedDate)

        let date = Date(timeIntervalSince1970: 1_234_567.89)
        let archive = Data([0xDE, 0xAD, 0xBE, 0xEF])
        instance.setSkybridgeMetadata(SkybridgeMetadata(ckRecordData: archive, syncLastModifiedDate: date))
        XCTAssertNotNil(instance.skybridgeMetadata)

        let decoded = instance.skybridgeDecodedMetadata
        XCTAssertEqual(decoded.ckRecordData, archive)
        XCTAssertEqual(decoded.syncLastModifiedDate, date)

        // Clearing both fields collapses the column back to nil
        instance.setSkybridgeMetadata(SkybridgeMetadata())
        XCTAssertNil(instance.skybridgeMetadata)

        // Garbage data decodes as empty metadata rather than throwing or crashing
        let garbage = SkybridgeMetadata.decode(Data([0x00, 0x01, 0x02]))
        XCTAssertNil(garbage.ckRecordData)
        XCTAssertNil(garbage.syncLastModifiedDate)
    }
#endif

    // MARK: Miscellaneous

    // The prepared-statement cache must survive exceeding its entry cap.
    func testStatementCacheCap() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await TestModel(id: 1, title: "still here", url: TestData.randomURL).write(to: db)

        for i in 0..<600 {
            _ = try await db.query("SELECT \(i)")
        }

        let survivor = try await TestModel.read(from: db, id: 1)
        XCTAssertEqual(survivor?.title, "still here")
        await db.close()
    }
}
