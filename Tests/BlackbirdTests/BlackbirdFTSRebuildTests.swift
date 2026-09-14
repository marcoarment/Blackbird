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
//  BlackbirdFTSRebuildTests.swift
//  Created by Marco Arment on 8/2/26.
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

import XCTest
@testable import Blackbird

fileprivate struct FTSIncrementalModel: BlackbirdModel {
    static let fullTextIndexRebuildsIncrementally = true

    static let fullTextSearchableColumns: FullTextIndex = [
        \.$title : .text,
        \.$body  : .text,
    ]

    @BlackbirdColumn var id: Int
    @BlackbirdColumn var title: String
    @BlackbirdColumn var body: String
}


fileprivate func XCTAssertEqualAsync<T: Equatable>(_ expression: @autoclosure () async throws -> T, _ expected: T, _ message: String = "", file: StaticString = #filePath, line: UInt = #line) async {
    do {
        let value = try await expression()
        XCTAssertEqual(value, expected, message, file: file, line: line)
    } catch {
        XCTFail("threw \(error): \(message)", file: file, line: line)
    }
}

final class BlackbirdFTSRebuildTests: XCTestCase {
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

    private func writeRows(_ db: Blackbird.Database, count: Int) async throws {
        try await db.transaction { core in
            for i in 0..<count {
                try FTSIncrementalModel(id: i, title: "alpha item \(i)", body: "bravo content number \(i)").write(to: core)
            }
        }
    }

    // A missing or altered trigger is repaired in place without rebuilding the index.
    func testTriggerOnlyChangeDoesNotRebuildIndex() async throws {
        let db1 = try Blackbird.Database(path: sqliteFilename)
        let resolution1 = try await FTSModel.resolveSchema(in: db1)
        XCTAssert(resolution1.contains(.migratedFullTextIndex))

        try await db1.transaction { core in
            for i in 0..<50 {
                try FTSModel(id: i, title: "seaplane hangar \(i)", url: URL(string: "https://example.com/\(i)")!, description: "d\(i)", keywords: "k", category: i % 5).write(to: core)
            }
        }
        await XCTAssertEqualAsync(try await FTSModel.fullTextSearch(from: db1, matching: .match("seaplane")).count, 50)

        // Simulate an outdated database: drop the update trigger, then retitle a row while
        // index maintenance is broken (so the index still holds its old text)
        try await db1.execute("DROP TRIGGER `FTSModel+FTSUpdate`")
        try await db1.query("UPDATE FTSModel SET title = 'gyrocopter pad 10' WHERE id = 10")
        await db1.close()

        let db2 = try Blackbird.Database(path: sqliteFilename)
        let resolution2 = try await FTSModel.resolveSchema(in: db2)
        XCTAssert(resolution2.contains(.updatedFullTextIndexTriggers))
        XCTAssert(!resolution2.contains(.migratedFullTextIndex), "trigger-only changes must not rebuild the index")
        XCTAssert(!resolution2.contains(.migratedTable))

        // The index content was untouched: it still holds id 10's PRE-update text,
        // proving the trigger refresh didn't rebuild anything
        await XCTAssertEqualAsync(try await FTSModel.fullTextSearch(from: db2, matching: .match("seaplane")).count, 50)
        await XCTAssertEqualAsync(try await FTSModel.fullTextSearch(from: db2, matching: .match("gyrocopter")).count, 0)

        // ...triggers work again...
        try await FTSModel.query(in: db2, "UPDATE $T SET title = 'zeppelin dock' WHERE id = 3")
        await XCTAssertEqualAsync(try await FTSModel.fullTextSearch(from: db2, matching: .match("zeppelin")).count, 1)
        await XCTAssertEqualAsync(try await FTSModel.fullTextSearch(from: db2, matching: .match("seaplane")).count, 49)

        // ...and a directly-requested incremental rebuild repairs the stale entry
        try await FTSModel.beginIncrementalFullTextIndexRebuild(in: db2)
        let progress = try await FTSModel.continueIncrementalFullTextIndexRebuild(in: db2, timeLimit: 60, batchSize: 10)
        guard case .done = progress else { return XCTFail("expected .done, got \(progress)") }
        await XCTAssertEqualAsync(try await FTSModel.fullTextSearch(from: db2, matching: .match("seaplane")).count, 48 /* ids 3 and 10 retitled */)
        await XCTAssertEqualAsync(try await FTSModel.fullTextSearch(from: db2, matching: .match("gyrocopter")).count, 1)

        // Back to a fully-normal state
        let resolution3 = try await FTSModel.resolveSchema(in: db2)
        XCTAssert(!resolution3.contains(.updatedFullTextIndexTriggers))
        XCTAssert(!resolution3.contains(.migratedFullTextIndex))
        await db2.close()
    }

    // A model opted into incremental rebuilds gets a queued rebuild at schema resolution
    // instead of a synchronous whole-table re-index.
    func testIncrementalRebuildAtSchemaResolution() async throws {
        let count = 250

        let db1 = try Blackbird.Database(path: sqliteFilename)
        _ = try await FTSIncrementalModel.resolveSchema(in: db1)
        try await writeRows(db1, count: count)
        await XCTAssertEqualAsync(try await FTSIncrementalModel.fullTextSearch(from: db1, matching: .match("alpha")).count, count)

        // Force a rebuild condition
        try await db1.execute("DROP TABLE `FTSIncrementalModel+FTS`")
        await db1.close()

        let db2 = try Blackbird.Database(path: sqliteFilename)
        let resolution = try await FTSIncrementalModel.resolveSchema(in: db2)
        XCTAssert(resolution.contains(.migratedFullTextIndex))

        // The rebuild was queued, not performed: index is empty, pending table is full
        await XCTAssertEqualAsync(try await FTSIncrementalModel.fullTextSearch(from: db2, matching: .match("alpha")).count, 0)
        let pendingCount = try await db2.query("SELECT COUNT(*) AS c FROM `FTSIncrementalModel+FTSRebuildPending`").first?["c"]?.int64Value
        XCTAssertEqual(pendingCount, Int64(count))

        // Writes during the rebuild are searchable immediately, in every pending-state combination:
        // a brand-new row, an update to a not-yet-indexed (pending) row, and deletes of both kinds
        try await FTSIncrementalModel(id: 9999, title: "zebrahorse arrives", body: "new while rebuilding").write(to: db2)
        await XCTAssertEqualAsync(try await FTSIncrementalModel.fullTextSearch(from: db2, matching: .match("zebrahorse")).count, 1)

        try await FTSIncrementalModel.query(in: db2, "UPDATE $T SET title = 'flamingopelican item' WHERE id = 100")
        await XCTAssertEqualAsync(try await FTSIncrementalModel.fullTextSearch(from: db2, matching: .match("flamingopelican")).count, 1)

        try await FTSIncrementalModel.query(in: db2, "DELETE FROM $T WHERE id = 101")           // pending: never indexed
        try await FTSIncrementalModel.query(in: db2, "DELETE FROM $T WHERE id = 9999")          // indexed by trigger
        await XCTAssertEqualAsync(try await FTSIncrementalModel.fullTextSearch(from: db2, matching: .match("zebrahorse")).count, 0)

        // A tiny time budget stops early with rows remaining
        let partial = try await FTSIncrementalModel.continueIncrementalFullTextIndexRebuild(in: db2, timeLimit: 0, batchSize: 10)
        guard case let .timeLimitReached(rowsIndexed1, rowsRemaining1) = partial else { return XCTFail("expected .timeLimitReached, got \(partial)") }
        XCTAssert(rowsIndexed1 > 0 && rowsRemaining1 > 0)

        // The queued state survives closing and reopening without restarting
        await db2.close()
        let db3 = try Blackbird.Database(path: sqliteFilename)
        let resolution3 = try await FTSIncrementalModel.resolveSchema(in: db3)
        XCTAssert(!resolution3.contains(.migratedFullTextIndex), "an intact in-progress rebuild must not restart at schema resolution")

        let finish = try await FTSIncrementalModel.continueIncrementalFullTextIndexRebuild(in: db3, timeLimit: 60, batchSize: 25)
        guard case let .done(rowsIndexed2) = finish else { return XCTFail("expected .done, got \(finish)") }
        XCTAssertEqual(rowsIndexed1 + rowsIndexed2, Int64(count) - 2 /* id 100 and 101 left pending via trigger/delete */ )

        // Index is complete and exact: no duplicates for the mid-rebuild update, deleted rows absent
        await XCTAssertEqualAsync(try await FTSIncrementalModel.fullTextSearch(from: db3, matching: .match("alpha")).count, count - 2 /* id 100 retitled, id 101 deleted */)
        await XCTAssertEqualAsync(try await FTSIncrementalModel.fullTextSearch(from: db3, matching: .match("flamingopelican")).count, 1)
        await XCTAssertEqualAsync(try await FTSIncrementalModel.fullTextSearch(from: db3, matching: .match("bravo")).count, count - 1 /* id 101 deleted */)

        // Pending table is gone, triggers are back to normal, further calls are no-ops
        let pendingTables = try await db3.query("SELECT name FROM sqlite_master WHERE name = 'FTSIncrementalModel+FTSRebuildPending'")
        XCTAssert(pendingTables.isEmpty)
        await XCTAssertEqualAsync(try await FTSIncrementalModel.continueIncrementalFullTextIndexRebuild(in: db3, timeLimit: 1), .notInProgress)

        let resolution4 = try await FTSIncrementalModel.resolveSchema(in: db3)
        XCTAssert(!resolution4.contains(.migratedFullTextIndex))
        XCTAssert(!resolution4.contains(.updatedFullTextIndexTriggers))

        // Normal maintenance still works post-rebuild
        try await FTSIncrementalModel(id: 10000, title: "quokkawombat", body: "post rebuild").write(to: db3)
        await XCTAssertEqualAsync(try await FTSIncrementalModel.fullTextSearch(from: db3, matching: .match("quokkawombat")).count, 1)
        await db3.close()
    }

    // An empty table's incremental rebuild completes immediately.
    func testIncrementalRebuildEmptyTable() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        _ = try await FTSIncrementalModel.resolveSchema(in: db)

        try await FTSIncrementalModel.beginIncrementalFullTextIndexRebuild(in: db)
        let progress = try await FTSIncrementalModel.continueIncrementalFullTextIndexRebuild(in: db, timeLimit: 60)
        guard case .done(rowsIndexed: 0) = progress else { return XCTFail("expected .done(0), got \(progress)") }

        try await FTSIncrementalModel(id: 1, title: "walrusnarwhal", body: "b").write(to: db)
        await XCTAssertEqualAsync(try await FTSIncrementalModel.fullTextSearch(from: db, matching: .match("walrusnarwhal")).count, 1)
        await db.close()
    }
}
