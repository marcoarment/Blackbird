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
//  BlackbirdQueryEdgeTests.swift
//
//  Edge-case tests for structured-query generation (NULL semantics, deep
//  boolean nesting, .literal parenthesization, .valueIn membership, orderBy,
//  limit, structured update/delete change reporting, primary-key batch reads,
//  raw-query argument binding) and full-text search (query escaping of
//  degenerate/unicode/operator input, prefix matching, trigger integrity,
//  bm25 column weighting).
//

import XCTest
import Combine
@testable import Blackbird

// MARK: - Models

struct EdgeQueryModel: BlackbirdModel {
    static let tableName = "EdgeQueryModel"

    @BlackbirdColumn var id: Int
    @BlackbirdColumn var name: String
    @BlackbirdColumn var score: Double
    @BlackbirdColumn var stamp: Date
    @BlackbirdColumn var optText: String?
    @BlackbirdColumn var optInt: Int?
}

struct EdgeQueryFTSModel: BlackbirdModel {
    static let tableName = "EdgeQueryFTSModel"
    static let fullTextSearchableColumns: FullTextIndex = [
        \.$title    : .text(weight: 3.0),
        \.$body     : .text,
        \.$category : .filterOnly,
    ]

    @BlackbirdColumn var id: Int
    @BlackbirdColumn var title: String
    @BlackbirdColumn var body: String
    @BlackbirdColumn var category: Int
}

// MARK: - Tests

final class BlackbirdQueryEdgeTests: XCTestCase, @unchecked Sendable {
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

    // Standard fixture:
    //  id | name      | score | stamp (epoch) | optText | optInt
    //   1 | "alpha"   |  1.0  |     1000      |  "a"    |  10
    //   2 | "bravo"   |  2.5  |     2000      |  NULL   |  20
    //   3 | "charlie" |  2.5  |     3000      |  "c"    |  NULL
    //   4 | "delta"   |  4.0  |     4000      |  NULL   |  NULL
    //   5 | "echo"    |  5.5  |     5000      |  "e"    |  50
    private func seedStandardRows(_ db: Blackbird.Database) async throws {
        try await EdgeQueryModel(id: 1, name: "alpha",   score: 1.0, stamp: Date(timeIntervalSince1970: 1000), optText: "a", optInt: 10).write(to: db)
        try await EdgeQueryModel(id: 2, name: "bravo",   score: 2.5, stamp: Date(timeIntervalSince1970: 2000), optText: nil, optInt: 20).write(to: db)
        try await EdgeQueryModel(id: 3, name: "charlie", score: 2.5, stamp: Date(timeIntervalSince1970: 3000), optText: "c", optInt: nil).write(to: db)
        try await EdgeQueryModel(id: 4, name: "delta",   score: 4.0, stamp: Date(timeIntervalSince1970: 4000), optText: nil, optInt: nil).write(to: db)
        try await EdgeQueryModel(id: 5, name: "echo",    score: 5.5, stamp: Date(timeIntervalSince1970: 5000), optText: "e", optInt: 50).write(to: db)
    }

    private func ids(_ instances: [EdgeQueryModel]) -> [Int] { instances.map(\.id) }
    private func idSet(_ instances: [EdgeQueryModel]) -> Set<Int> { Set(instances.map(\.id)) }
    // NOTE: deliberately resolves instances through instance(from:) rather than
    // preloadedInstance — see testFTSPreloadedInstancesPopulatedByDefault for why
    // preloadedInstance is broken (real bug) for models with an INTEGER PRIMARY KEY.
    private func resultIDs(_ results: [BlackbirdModelSearchResult<EdgeQueryFTSModel>], in db: Blackbird.Database) async throws -> [Int] {
        var ids: [Int] = []
        for result in results {
            if let instance = try await result.instance(from: db) { ids.append(instance.id) }
        }
        return ids
    }

    // MARK: - Structured query DSL

    // == nil must match only NULL rows; != nil only non-NULL rows.
    // Equality against a value uses IS NOT DISTINCT FROM: NULL rows must not match == "a",
    // and (Blackbird's documented IS DISTINCT FROM semantics) NULL rows MUST match != "a".
    func testOptionalColumnNullSemantics() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await seedStandardRows(db)

        let isNull = try await EdgeQueryModel.read(from: db, matching: \.$optText == nil)
        XCTAssertEqual(idSet(isNull), [2, 4])

        let isNotNull = try await EdgeQueryModel.read(from: db, matching: \.$optText != nil)
        XCTAssertEqual(idSet(isNotNull), [1, 3, 5])

        let equalsA = try await EdgeQueryModel.read(from: db, matching: \.$optText == "a")
        XCTAssertEqual(idSet(equalsA), [1], "NULL rows must not match == 'a'")

        // IS DISTINCT FROM: NULL is distinct from 'a', so NULL rows are included
        let notEqualsA = try await EdgeQueryModel.read(from: db, matching: \.$optText != "a")
        XCTAssertEqual(idSet(notEqualsA), [2, 3, 4, 5], "IS DISTINCT FROM semantics: NULL rows must match != 'a'")

        // Same semantics on an optional Int column
        let intNull = try await EdgeQueryModel.read(from: db, matching: \.$optInt == nil)
        XCTAssertEqual(idSet(intNull), [3, 4])
        let intNotTwenty = try await EdgeQueryModel.read(from: db, matching: \.$optInt != 20)
        XCTAssertEqual(idSet(intNotTwenty), [1, 3, 4, 5])
        await db.close()
    }

    // >, >=, <, <= on Int, Double, String, and Date columns, including exact boundary equality.
    func testComparisonOperatorBoundaries() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await seedStandardRows(db)

        // Int
        let gt = try await EdgeQueryModel.read(from: db, matching: \.$id > 3)
        XCTAssertEqual(idSet(gt), [4, 5])
        let gte = try await EdgeQueryModel.read(from: db, matching: \.$id >= 3)
        XCTAssertEqual(idSet(gte), [3, 4, 5])
        let lt = try await EdgeQueryModel.read(from: db, matching: \.$id < 3)
        XCTAssertEqual(idSet(lt), [1, 2])
        let lte = try await EdgeQueryModel.read(from: db, matching: \.$id <= 3)
        XCTAssertEqual(idSet(lte), [1, 2, 3])

        // Double: boundary at 2.5 (two rows share it)
        let dGte = try await EdgeQueryModel.read(from: db, matching: \.$score >= 2.5)
        XCTAssertEqual(idSet(dGte), [2, 3, 4, 5])
        let dGt = try await EdgeQueryModel.read(from: db, matching: \.$score > 2.5)
        XCTAssertEqual(idSet(dGt), [4, 5])
        let dLte = try await EdgeQueryModel.read(from: db, matching: \.$score <= 2.5)
        XCTAssertEqual(idSet(dLte), [1, 2, 3])

        // String (BINARY collation, all-lowercase fixture)
        let sGte = try await EdgeQueryModel.read(from: db, matching: \.$name >= "charlie")
        XCTAssertEqual(idSet(sGte), [3, 4, 5])
        let sLt = try await EdgeQueryModel.read(from: db, matching: \.$name < "bravo")
        XCTAssertEqual(idSet(sLt), [1])

        // Date: stored as REAL epoch seconds; boundary must be exact
        let boundary = Date(timeIntervalSince1970: 3000)
        let dateLte = try await EdgeQueryModel.read(from: db, matching: \.$stamp <= boundary)
        XCTAssertEqual(idSet(dateLte), [1, 2, 3])
        let dateGt = try await EdgeQueryModel.read(from: db, matching: \.$stamp > boundary)
        XCTAssertEqual(idSet(dateGt), [4, 5])
        let dateEq = try await EdgeQueryModel.read(from: db, matching: \.$stamp == boundary)
        XCTAssertEqual(idSet(dateEq), [3])
        await db.close()
    }

    // Deeply nested boolean expressions verified against hand-computed row sets.
    func testDeeplyNestedBooleanExpressions() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await seedStandardRows(db)

        // (a && b) || (c && d):
        // left: id <= 2 AND optText IS NULL -> {2}
        // right: score > 4.0 AND optInt IS NOT NULL -> {5}
        let twoBranch = try await EdgeQueryModel.read(
            from: db,
            matching: (\.$id <= 2 && \.$optText == nil) || (\.$score > 4.0 && \.$optInt != nil)
        )
        XCTAssertEqual(idSet(twoBranch), [2, 5])

        // 4+ levels with negation:
        // NOT( (id == 1 OR id == 2) AND NOT(optText IS NULL) ) AND score < 5.0
        // inner AND -> {1}; NOT -> {2,3,4,5}; AND score < 5.0 -> {2,3,4}
        let deep = try await EdgeQueryModel.read(
            from: db,
            matching: !((\.$id == 1 || \.$id == 2) && !(\.$optText == nil)) && \.$score < 5.0
        )
        XCTAssertEqual(idSet(deep), [2, 3, 4])

        // Double negation is the identity
        let doubleNot = try await EdgeQueryModel.read(from: db, matching: !(!(\.$id == 3)))
        XCTAssertEqual(idSet(doubleNot), [3])

        // A 5-level monster, hand-computed:
        // ((id > 1 && id < 5) || name == "alpha") && !(optInt == nil && score >= 4.0)
        // left OR: {2,3,4} U {1} = {1,2,3,4}; right NOT of {4} = {1,2,3,5}; AND -> {1,2,3}
        let monster = try await EdgeQueryModel.read(
            from: db,
            matching: ((\.$id > 1 && \.$id < 5) || \.$name == "alpha") && !(\.$optInt == nil && \.$score >= 4.0)
        )
        XCTAssertEqual(idSet(monster), [1, 2, 3])
        await db.close()
    }

    // .all combined in every position, plus the !(.all) edge (must match nothing).
    func testMatchAllCombiningPositions() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await seedStandardRows(db)

        let allLeadingAnd = try await EdgeQueryModel.read(from: db, matching: .all && \.$id == 2)
        XCTAssertEqual(idSet(allLeadingAnd), [2])

        let allTrailingAnd = try await EdgeQueryModel.read(from: db, matching: \.$id == 2 && .all)
        XCTAssertEqual(idSet(allTrailingAnd), [2])

        let allLeadingOr = try await EdgeQueryModel.read(from: db, matching: .all || \.$id == 2)
        XCTAssertEqual(idSet(allLeadingOr), [1, 2, 3, 4, 5], ".all OR x must match everything")

        let allTrailingOr = try await EdgeQueryModel.read(from: db, matching: \.$id == 2 || .all)
        XCTAssertEqual(idSet(allTrailingOr), [1, 2, 3, 4, 5], "x OR .all must match everything")

        let allBoth = try await EdgeQueryModel.read(from: db, matching: .all && .all)
        XCTAssertEqual(allBoth.count, 5)

        // Nested: (.all && x) || (y && .all)
        let nested = try await EdgeQueryModel.read(from: db, matching: (.all && \.$id == 1) || (\.$id == 5 && .all))
        XCTAssertEqual(idSet(nested), [1, 5])

        // NOT(.all) compiles to FALSE: must match nothing
        let notAll = try await EdgeQueryModel.read(from: db, matching: !(BlackbirdModelColumnExpression<EdgeQueryModel>.all))
        XCTAssertEqual(notAll.count, 0, "!(.all) must match zero rows")

        let countAll = try await EdgeQueryModel.count(in: db, matching: .all)
        XCTAssertEqual(countAll, 5)
        await db.close()
    }

    // .literal with arguments must stay parenthesized inside complex nestings.
    func testLiteralExpressionComplexNesting() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await seedStandardRows(db)

        // OR inside the literal must not leak out of the enclosing AND
        let andBound = try await EdgeQueryModel.read(from: db, matching: .literal("id = ? OR id = ?", 1, 2) && \.$name == "bravo")
        XCTAssertEqual(idSet(andBound), [2])

        // Literal in the middle of a three-way AND chain
        let midChain = try await EdgeQueryModel.read(from: db, matching: \.$score > 0 && .literal("id % 2 = ?", 0) && \.$id < 4)
        XCTAssertEqual(idSet(midChain), [2])

        // Negated literal (no arguments)
        let negated = try await EdgeQueryModel.read(from: db, matching: !(BlackbirdModelColumnExpression<EdgeQueryModel>.literal("id = 1 OR id = 2")))
        XCTAssertEqual(idSet(negated), [3, 4, 5])

        // Literal on both sides of an OR, one nested under an AND
        let bothSides = try await EdgeQueryModel.read(
            from: db,
            matching: .literal("id = ?", 1) || (\.$id == 2 && .literal("name = ? OR name = ?", "bravo", "zzz"))
        )
        XCTAssertEqual(idSet(bothSides), [1, 2])

        // Argument ordering across mixed literal/keypath expressions
        let mixedArgs = try await EdgeQueryModel.read(
            from: db,
            matching: \.$score >= 2.5 && .literal("id > ? AND id < ?", 1, 5) && \.$optText != nil
        )
        XCTAssertEqual(idSet(mixedArgs), [3])
        await db.close()
    }

    // .valueIn: empty collection must match nothing (SQLite allows the empty IN () list),
    // single element, large collection, negation, and combination with other clauses.
    func testValueInMembership() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await seedStandardRows(db)

        let empty = try await EdgeQueryModel.read(from: db, matching: .valueIn(\.$id, [Int]()))
        XCTAssertEqual(empty.count, 0, "empty .valueIn collection must match nothing")

        let single = try await EdgeQueryModel.read(from: db, matching: .valueIn(\.$id, [3]))
        XCTAssertEqual(idSet(single), [3])

        let large = try await EdgeQueryModel.read(from: db, matching: .valueIn(\.$id, Array(1...1000)))
        XCTAssertEqual(idSet(large), [1, 2, 3, 4, 5])

        let combined = try await EdgeQueryModel.read(from: db, matching: .valueIn(\.$id, [1, 2, 3]) && \.$optText != nil)
        XCTAssertEqual(idSet(combined), [1, 3])

        let negatedIn = try await EdgeQueryModel.read(from: db, matching: !(BlackbirdModelColumnExpression<EdgeQueryModel>.valueIn(\.$id, [1, 2])))
        XCTAssertEqual(idSet(negatedIn), [3, 4, 5])

        // String-typed membership
        let names = try await EdgeQueryModel.read(from: db, matching: .valueIn(\.$name, ["alpha", "echo", "nope"]))
        XCTAssertEqual(idSet(names), [1, 5])
        await db.close()
    }

    // orderBy: multiple columns, mixed directions, and NULL placement (SQLite: NULLs sort first ascending).
    func testOrderByMultipleColumnsAndNulls() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await seedStandardRows(db)

        // score DESC, id ASC — the 2.5 tie (ids 2, 3) must break ascending
        let scoreDesc = try await EdgeQueryModel.read(from: db, orderBy: .descending(\.$score), .ascending(\.$id))
        XCTAssertEqual(ids(scoreDesc), [5, 4, 2, 3, 1])

        // optInt ASC: NULLs (ids 3, 4) first, tie-broken by id ASC, then 10, 20, 50
        let nullsFirst = try await EdgeQueryModel.read(from: db, orderBy: .ascending(\.$optInt), .ascending(\.$id))
        XCTAssertEqual(ids(nullsFirst), [3, 4, 1, 2, 5])

        // optInt DESC: 50, 20, 10, then NULLs last, tie-broken by id ASC
        let nullsLast = try await EdgeQueryModel.read(from: db, orderBy: .descending(\.$optInt), .ascending(\.$id))
        XCTAssertEqual(ids(nullsLast), [5, 2, 1, 3, 4])
        await db.close()
    }

    // limit: 0 must return empty, 1, and larger than the row count.
    func testLimitEdges() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await seedStandardRows(db)

        let zero = try await EdgeQueryModel.read(from: db, orderBy: .ascending(\.$id), limit: 0)
        XCTAssertEqual(zero.count, 0, "limit: 0 must return no rows")

        let one = try await EdgeQueryModel.read(from: db, orderBy: .ascending(\.$id), limit: 1)
        XCTAssertEqual(ids(one), [1])

        let over = try await EdgeQueryModel.read(from: db, orderBy: .ascending(\.$id), limit: 100)
        XCTAssertEqual(ids(over), [1, 2, 3, 4, 5])

        let matchingLimited = try await EdgeQueryModel.read(from: db, matching: \.$id > 2, orderBy: .ascending(\.$id), limit: 2)
        XCTAssertEqual(ids(matchingLimited), [3, 4])
        await db.close()
    }

    // update(matching:) setting a column to its current value: Blackbird's update-WHERE
    // auto-optimization (BlackbirdModelStructuredQuerying.swift:132) deliberately excludes
    // already-matching rows, so a true no-op update publishes NO change event.
    // An actual value change must publish a keyed (non-nil changedPrimaryKeys) event.
    func testUpdateMatchingSameValueChangeReporting() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await seedStandardRows(db)
        try await Task.sleep(nanoseconds: 300_000_000) // let seed-write events drain

        let received = Blackbird.Locked<[Blackbird.ModelChange<EdgeQueryModel>]>([])
        let subscription = EdgeQueryModel.changePublisher(in: db).sink { change in
            received.withLock { $0.append(change) }
        }
        defer { subscription.cancel() }

        // No-op update: name is already "alpha"
        try await EdgeQueryModel.update(in: db, set: [ \.$name : "alpha" ], matching: \.$id == 1)
        try await Task.sleep(nanoseconds: 700_000_000)
        let afterNoOp = received.value
        XCTAssertEqual(afterNoOp.count, 0, "no-op update (set to current value) should publish no change event (documented auto-optimization behavior)")

        // Real update: must publish a keyed change for exactly id 1 with the changed column
        try await EdgeQueryModel.update(in: db, set: [ \.$name : "alpha-2" ], matching: \.$id == 1)
        try await Task.sleep(nanoseconds: 700_000_000)
        let afterReal = received.value
        XCTAssertEqual(afterReal.count, 1, "real update must publish exactly one change event")
        if let change = afterReal.first {
            let expectedKeys: Blackbird.PrimaryKeyValues = [[.integer(1)]]
            XCTAssertEqual(change.changedPrimaryKeys, expectedKeys, "update change must be keyed to the specific primary key")
            let nameChanged = change.hasColumnChanged("name")
            XCTAssert(nameChanged)
        }
        await db.close()
    }

    // Structured update setting an optional column to NULL.
    func testStructuredUpdateSetOptionalToNil() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await seedStandardRows(db)

        try await EdgeQueryModel.update(in: db, set: [ \.$optText : nil as String? ], matching: \.$id == 1)

        let updated = try await EdgeQueryModel.read(from: db, id: 1)
        XCTAssertNotNil(updated)
        XCTAssertNil(updated?.optText, "structured update must be able to set an optional column to NULL")
        XCTAssertEqual(updated?.name, "alpha", "other columns must be untouched")

        // Setting an already-NULL column to nil is a no-op (auto-optimization) and must not throw
        try await EdgeQueryModel.update(in: db, set: [ \.$optText : nil as String? ], matching: \.$id == 2)
        let stillNull = try await EdgeQueryModel.read(from: db, id: 2)
        XCTAssertNil(stillNull?.optText)
        await db.close()
    }

    // delete(matching:) matching zero rows must not publish a change event
    // (over-reporting would cause spurious cache invalidation and UI refreshes).
    func testDeleteMatchingZeroRowsPublishesNoChange() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await seedStandardRows(db)
        try await Task.sleep(nanoseconds: 300_000_000)

        let received = Blackbird.Locked<[Blackbird.ModelChange<EdgeQueryModel>]>([])
        let subscription = EdgeQueryModel.changePublisher(in: db).sink { change in
            received.withLock { $0.append(change) }
        }
        defer { subscription.cancel() }

        try await EdgeQueryModel.delete(from: db, matching: \.$id == 999)
        try await Task.sleep(nanoseconds: 700_000_000)
        let afterMiss = received.value
        XCTAssertEqual(afterMiss.count, 0, "delete matching zero rows must not publish a change event")

        // Sanity: a delete that does match must publish a keyed change
        try await EdgeQueryModel.delete(from: db, matching: \.$id == 5)
        try await Task.sleep(nanoseconds: 700_000_000)
        let afterHit = received.value
        XCTAssertEqual(afterHit.count, 1)
        if let change = afterHit.first {
            let expectedKeys: Blackbird.PrimaryKeyValues = [[.integer(5)]]
            XCTAssertEqual(change.changedPrimaryKeys, expectedKeys)
        }
        await db.close()
    }

    // count(in:matching:) must agree with read(from:matching:).count across varied expressions.
    func testCountMatchesReadCount() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await seedStandardRows(db)

        let expressions: [(String, BlackbirdModelColumnExpression<EdgeQueryModel>)] = [
            ("all", .all),
            ("score > 2", \.$score > 2),
            ("optText NULL and score < 5", \.$optText == nil && \.$score < 5.0),
            ("valueIn", .valueIn(\.$id, [1, 3, 5, 7])),
            ("literal mod", .literal("id % 2 = ?", 1)),
            ("nested", (\.$id > 1 && \.$id < 5) || \.$name == "echo"),
            ("not", !(\.$optInt == nil)),
        ]

        for (label, expression) in expressions {
            let counted = try await EdgeQueryModel.count(in: db, matching: expression)
            let readCount = try await EdgeQueryModel.read(from: db, matching: expression).count
            XCTAssertEqual(counted, readCount, "count(in:matching:) disagrees with read().count for expression: \(label)")
        }

        let countNoMatch = try await EdgeQueryModel.count(in: db, matching: \.$id == 42)
        XCTAssertEqual(countNoMatch, 0)
        await db.close()
    }

    // read(from:primaryKeys:): empty array, duplicate keys, missing keys, and preserveOrder.
    func testReadPrimaryKeysEdges() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await seedStandardRows(db)

        let empty = try await EdgeQueryModel.read(from: db, primaryKeys: [Int]())
        XCTAssertEqual(empty.count, 0, "empty primaryKeys array must return no rows (and not be a SQL syntax error)")

        // Duplicates without preserveOrder: SQL IN dedupes, so one instance per matching row
        let dupes = try await EdgeQueryModel.read(from: db, primaryKeys: [3, 3, 3])
        XCTAssertEqual(ids(dupes), [3], "duplicate keys must not produce duplicate instances")

        // preserveOrder with unordered input, including a missing key
        let ordered = try await EdgeQueryModel.read(from: db, primaryKeys: [4, 999, 1, 3], preserveOrder: true)
        XCTAssertEqual(ids(ordered), [4, 1, 3], "preserveOrder must follow the input key order and skip missing keys")

        // preserveOrder with duplicate keys: documents actual behavior — the ordering pass
        // maps each input key to its instance, so a duplicated key yields a duplicated instance.
        let orderedDupes = try await EdgeQueryModel.read(from: db, primaryKeys: [2, 5, 2], preserveOrder: true)
        XCTAssertEqual(ids(orderedDupes), [2, 5, 2], "preserveOrder duplicates each duplicated input key (documented behavior)")
        await db.close()
    }

    // read(primaryKeys:) with 2,000 keys must work via internal chunking
    // (chunk size = maxQueryVariableCount / 2), and a raw query that exceeds
    // SQLITE_LIMIT_VARIABLE_NUMBER must throw cleanly rather than crash.
    func testReadManyPrimaryKeysAndVariableLimit() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        for i in 1...50 {
            try await EdgeQueryModel(id: i, name: "row \(i)", score: Double(i), stamp: Date(timeIntervalSince1970: Double(i)), optText: nil, optInt: nil).write(to: db)
        }

        let results = try await EdgeQueryModel.read(from: db, primaryKeys: Array(1...2000))
        XCTAssertEqual(idSet(results), Set(1...50))

        let limit = db.maxQueryVariableCount
        XCTAssert(limit > 0)

        // Directly exceeding SQLITE_LIMIT_VARIABLE_NUMBER must throw a clean Blackbird error
        // at prepare time ("too many SQL variables"), not crash or corrupt state.
        // Only probe when the limit is small enough to build the query quickly.
        if limit <= 40_000 {
            let placeholders = Array(repeating: "?", count: limit + 1).joined(separator: ",")
            await AssertThrowsErrorAsync(_ = try await db.query("SELECT id FROM EdgeQueryModel WHERE id IN (\(placeholders))", arguments: Array(repeating: 1 as Sendable, count: limit + 1)))
        }

        // The database must remain fully usable afterward
        let survivor = try await EdgeQueryModel.read(from: db, id: 25)
        XCTAssertEqual(survivor?.name, "row 25")
        await db.close()
    }

    // Named placeholders (:name, @name, $name) via the dictionary-arguments API.
    // A name not present in the query must throw queryArgumentNameError.
    // A placeholder with no supplied argument binds NULL (SQLite default) — documented here.
    func testNamedPlaceholders() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)

        let rows = try await db.query(
            "SELECT :a AS a, @b AS b, $c AS c",
            arguments: [":a": 1, "@b": "two", "$c": 3.5]
        )
        XCTAssertEqual(rows.count, 1)
        XCTAssertEqual(rows.first?["a"], .integer(1))
        XCTAssertEqual(rows.first?["b"], .text("two"))
        XCTAssertEqual(rows.first?["c"], .double(3.5))

        // Argument name that isn't a placeholder in the query: must throw cleanly
        do {
            _ = try await db.query("SELECT :a AS a", arguments: [":nope": 1])
            XCTFail("expected queryArgumentNameError for unknown placeholder name")
        } catch Blackbird.Database.Error.queryArgumentNameError(_, let name) {
            XCTAssertEqual(name, ":nope")
        }

        // Placeholder present in the query but missing from the dictionary: SQLite leaves it
        // unbound (NULL). Blackbird does not diagnose this — documented actual behavior.
        let missing = try await db.query("SELECT :a AS a, :b AS b", arguments: [":a": 5])
        XCTAssertEqual(missing.first?["a"], .integer(5))
        XCTAssertEqual(missing.first?["b"], .null, "unsupplied named placeholder binds NULL (no error; documented behavior)")
        await db.close()
    }

    // Wrong positional-argument counts, and cached-statement state afterward.
    func testPositionalArgumentCountMismatches() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        let sql = "SELECT ? AS a"

        // Too many positional arguments: must throw cleanly (SQLITE_RANGE on the extra bind)
        do {
            _ = try await db.query(sql, arguments: ["x", "y"])
            XCTFail("expected a bind error for too many positional arguments")
        } catch Blackbird.Database.Error.queryArgumentValueError { } // expected

        // The same SQL string (same cached statement) must still work with correct arguments
        let correct = try await db.query(sql, arguments: ["z"])
        XCTAssertEqual(correct.first?["a"], .text("z"), "cached statement must recover after a failed bind")

        // Cached-statement state probe: fail a bind again (index 1 gets bound to "p" before
        // index 2 throws), then execute the SAME SQL with NO arguments. A clean statement
        // would produce NULL; a stale leftover binding would leak "p" into this execution.
        do {
            _ = try await db.query(sql, arguments: ["p", "q"])
            XCTFail("expected a bind error")
        } catch Blackbird.Database.Error.queryArgumentValueError { } // expected

        let noArgs = try await db.query(sql)
        XCTAssertEqual(noArgs.first?["a"], .null, "bindings from a failed bind attempt leaked into the next execution of the cached statement")

        // Too few positional arguments: SQLite binds the missing parameter as NULL, no error —
        // documented actual behavior.
        let tooFew = try await db.query("SELECT ? AS a, ? AS b", arguments: ["only"])
        XCTAssertEqual(tooFew.first?["a"], .text("only"))
        XCTAssertEqual(tooFew.first?["b"], .null, "too-few positional args bind NULL (no error; documented behavior)")
        await db.close()
    }

    // MARK: - Full-text search

    // Degenerate inputs under the default escaping mode: empty string, whitespace-only,
    // and a lone quote character. These all escape to an empty FTS5 MATCH pattern —
    // they must not throw, and should sensibly match nothing.
    func testFTSDegenerateInputs() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await EdgeQueryFTSModel(id: 1, title: "hello world", body: "some body text", category: 1).write(to: db)
        try await EdgeQueryFTSModel(id: 2, title: "another title", body: "more words", category: 1).write(to: db)

        for degenerate in ["", "   \n\t ", "\""] {
            do {
                let results = try await EdgeQueryFTSModel.fullTextSearch(from: db, matching: .match(degenerate), options: .default)
                XCTAssertEqual(results.count, 0, "degenerate query \(degenerate.debugDescription) should match nothing")
            } catch {
                // Regression guard: empty/whitespace/quote-only input used to escape to an
                // empty string, and `MATCH ''` is an FTS5 syntax error — a user clearing a
                // search field would throw instead of getting no results.
                XCTFail("degenerate query \(degenerate.debugDescription) must not throw, got: \(error)")
            }
        }
        await db.close()
    }

    // A lone `*`, a unicode word (with diacritic folding), and an emoji must be treated
    // as literal text under the default escaping mode: no throw, sensible results.
    func testFTSStarUnicodeAndEmoji() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await EdgeQueryFTSModel(id: 1, title: "café review", body: "the best espresso", category: 1).write(to: db)
        try await EdgeQueryFTSModel(id: 2, title: "party 🎉 time", body: "celebration", category: 1).write(to: db)

        // Lone `*` escapes to the quoted phrase "*", which tokenizes to nothing: no throw
        let star = try await EdgeQueryFTSModel.fullTextSearch(from: db, matching: .match("*"), options: .default)
        XCTAssertEqual(star.count, 0, "a lone * must be literal (no prefix-everything explosion) and match nothing")

        // Unicode word matches directly...
        let cafe1 = try await EdgeQueryFTSModel.fullTextSearch(from: db, matching: .match("café"), options: .default)
        let cafe1IDs = try await resultIDs(cafe1, in: db)
        XCTAssertEqual(cafe1IDs, [1])
        // ...and via diacritic removal (tokenizer: unicode61 remove_diacritics 2)
        let cafe2 = try await EdgeQueryFTSModel.fullTextSearch(from: db, matching: .match("cafe"), options: .default)
        let cafe2IDs = try await resultIDs(cafe2, in: db)
        XCTAssertEqual(cafe2IDs, [1])

        // Emoji: unicode61 treats emoji as separators, so the query tokenizes to nothing.
        // Must not throw; matches nothing (documented behavior).
        let emoji = try await EdgeQueryFTSModel.fullTextSearch(from: db, matching: .match("🎉"), options: .default)
        XCTAssertEqual(emoji.count, 0, "emoji-only query tokenizes to nothing and matches nothing (documented behavior)")
        await db.close()
    }

    // Hyphenated words and apostrophe words must be treated as adjacent-token phrases.
    func testFTSHyphenAndApostropheWords() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await EdgeQueryFTSModel(id: 1, title: "full-text search rocks", body: "x", category: 1).write(to: db)
        try await EdgeQueryFTSModel(id: 2, title: "text becomes full backwards", body: "x", category: 1).write(to: db)
        try await EdgeQueryFTSModel(id: 3, title: "don't panic", body: "x", category: 1).write(to: db)
        try await EdgeQueryFTSModel(id: 4, title: "do not panic slowly", body: "x", category: 1).write(to: db)

        // "full-text" must behave as the phrase full+text: matches row 1, not row 2
        // (row 2 contains both tokens but not adjacent/in order)
        let hyphen = try await EdgeQueryFTSModel.fullTextSearch(from: db, matching: .match("full-text"), options: .default)
        let hyphenIDs = try await resultIDs(hyphen, in: db)
        XCTAssertEqual(hyphenIDs, [1])

        // "don't" tokenizes to don+t: matches row 3 only, and must not throw
        // despite the apostrophe being a phrase-delimiter character mid-word
        let apostrophe = try await EdgeQueryFTSModel.fullTextSearch(from: db, matching: .match("don't"), options: .default)
        let apostropheIDs = try await resultIDs(apostrophe, in: db)
        XCTAssertEqual(apostropheIDs, [3])
        await db.close()
    }

    // AND / OR / NOT / NEAR typed by a user must be literal terms under the default
    // escaping mode — not FTS5 operators.
    func testFTSOperatorWordsAreLiteralWhenEscaped() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await EdgeQueryFTSModel(id: 1, title: "alpha and beta", body: "x", category: 1).write(to: db)
        try await EdgeQueryFTSModel(id: 2, title: "alpha beta", body: "x", category: 1).write(to: db)
        try await EdgeQueryFTSModel(id: 3, title: "alpha or beta", body: "x", category: 1).write(to: db)
        try await EdgeQueryFTSModel(id: 4, title: "alpha not beta", body: "x", category: 1).write(to: db)
        try await EdgeQueryFTSModel(id: 5, title: "alpha near beta", body: "x", category: 1).write(to: db)

        // If AND were an operator, "alpha AND beta" would match all five rows.
        // As literal terms it requires the word "and": row 1 only.
        let andQuery = try await EdgeQueryFTSModel.fullTextSearch(from: db, matching: .match("alpha AND beta"), options: .default)
        let andIDs = try await resultIDs(andQuery, in: db)
        XCTAssertEqual(andIDs, [1], "escaped AND must be a literal term")

        let orQuery = try await EdgeQueryFTSModel.fullTextSearch(from: db, matching: .match("alpha OR beta"), options: .default)
        let orIDs = try await resultIDs(orQuery, in: db)
        XCTAssertEqual(orIDs, [3], "escaped OR must be a literal term")

        let notQuery = try await EdgeQueryFTSModel.fullTextSearch(from: db, matching: .match("alpha NOT beta"), options: .default)
        let notIDs = try await resultIDs(notQuery, in: db)
        XCTAssertEqual(notIDs, [4], "escaped NOT must be a literal term")

        let nearQuery = try await EdgeQueryFTSModel.fullTextSearch(from: db, matching: .match("NEAR"), options: .default)
        let nearIDs = try await resultIDs(nearQuery, in: db)
        XCTAssertEqual(nearIDs, [5], "escaped NEAR must be a literal term, not a syntax error")

        // Contrast: in raw-syntax mode, AND is a real operator and matches every row with both terms
        let rawAnd = try await EdgeQueryFTSModel.fullTextSearch(from: db, matching: .match("alpha AND beta", syntaxMode: .allowFullQuerySyntax), options: .default)
        let rawAndIDs = try await resultIDs(rawAnd, in: db)
        XCTAssertEqual(Set(rawAndIDs), [1, 2, 3, 4, 5])
        await db.close()
    }

    // Prefix-match mode: partial words, trailing whitespace, trailing quoted phrase, single character.
    func testFTSPrefixMatchMode() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await EdgeQueryFTSModel(id: 1, title: "accidental tech podcast", body: "x", category: 1).write(to: db)
        try await EdgeQueryFTSModel(id: 2, title: "technology news", body: "x", category: 1).write(to: db)
        try await EdgeQueryFTSModel(id: 3, title: "tech corner", body: "x", category: 1).write(to: db)

        // Partial word: "tech"* matches tech, technology
        let prefix = try await EdgeQueryFTSModel.fullTextSearch(from: db, matching: .match("tech", syntaxMode: .escapeQuerySyntaxAndPrefixMatchLastPhrase), options: .default)
        let prefixIDs = try await resultIDs(prefix, in: db)
        XCTAssertEqual(Set(prefixIDs), [1, 2, 3])

        // Default (non-prefix) escaping only matches the exact token
        let exact = try await EdgeQueryFTSModel.fullTextSearch(from: db, matching: .match("tech"), options: .default)
        let exactIDs = try await resultIDs(exact, in: db)
        XCTAssertEqual(Set(exactIDs), [1, 3], "non-prefix mode must not match 'technology'")

        // Longer partial word
        let techno = try await EdgeQueryFTSModel.fullTextSearch(from: db, matching: .match("techno", syntaxMode: .escapeQuerySyntaxAndPrefixMatchLastPhrase), options: .default)
        let technoIDs = try await resultIDs(techno, in: db)
        XCTAssertEqual(Set(technoIDs), [2])

        // Input ending in whitespace: the last phrase is still prefix-matched (documented behavior)
        let trailingSpace = try await EdgeQueryFTSModel.fullTextSearch(from: db, matching: .match("tech ", syntaxMode: .escapeQuerySyntaxAndPrefixMatchLastPhrase), options: .default)
        let trailingSpaceIDs = try await resultIDs(trailingSpace, in: db)
        XCTAssertEqual(Set(trailingSpaceIDs), [1, 2, 3], "trailing whitespace still prefix-matches the last phrase (documented behavior)")

        // Input ending in a quoted phrase: no prefix star is appended, so "tech pod" must
        // match nothing ('pod' is not a full token anywhere)
        let quotedPhrase = try await EdgeQueryFTSModel.fullTextSearch(from: db, matching: .match("\"tech pod\"", syntaxMode: .escapeQuerySyntaxAndPrefixMatchLastPhrase), options: .default)
        XCTAssertEqual(quotedPhrase.count, 0, "a trailing QUOTED phrase must not be prefix-matched")

        // The same words unquoted DO prefix-match the last word: pod* -> podcast
        let unquoted = try await EdgeQueryFTSModel.fullTextSearch(from: db, matching: .match("tech pod", syntaxMode: .escapeQuerySyntaxAndPrefixMatchLastPhrase), options: .default)
        let unquotedIDs = try await resultIDs(unquoted, in: db)
        XCTAssertEqual(unquotedIDs, [1])

        // Single character
        let singleChar = try await EdgeQueryFTSModel.fullTextSearch(from: db, matching: .match("t", syntaxMode: .escapeQuerySyntaxAndPrefixMatchLastPhrase), options: .default)
        let singleCharIDs = try await resultIDs(singleChar, in: db)
        XCTAssertEqual(Set(singleCharIDs), [1, 2, 3])
        await db.close()
    }

    // match(column:) scoping, and .filterOnly columns used as WHERE filters alongside a match.
    // NOTE: .match(column:) on a .filterOnly column is a documented fatalError
    // ("can only be used on .text entries", BlackbirdModelStructuredQuerying.swift:846-853),
    // so that path is intentionally not executed here — it would crash the test process.
    func testFTSColumnScopedMatchAndFilterOnly() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await EdgeQueryFTSModel(id: 1, title: "zebra crossing", body: "plain content", category: 1).write(to: db)
        try await EdgeQueryFTSModel(id: 2, title: "plain title", body: "zebra herd", category: 2).write(to: db)

        // Column-scoped match: only title hits
        let titleOnly = try await EdgeQueryFTSModel.fullTextSearch(from: db, matching: .match(column: \.$title, "zebra"), options: .default)
        let titleOnlyIDs = try await resultIDs(titleOnly, in: db)
        XCTAssertEqual(titleOnlyIDs, [1])

        // Unscoped match hits both
        let both = try await EdgeQueryFTSModel.fullTextSearch(from: db, matching: .match("zebra"), options: .default)
        let bothIDs = try await resultIDs(both, in: db)
        XCTAssertEqual(Set(bothIDs), [1, 2])

        // .filterOnly column as a WHERE filter combined with the match
        let filtered = try await EdgeQueryFTSModel.fullTextSearch(from: db, matching: .match("zebra") && \.$category == 2, options: .default)
        let filteredIDs = try await resultIDs(filtered, in: db)
        XCTAssertEqual(filteredIDs, [2])

        // Filter that excludes everything
        let none = try await EdgeQueryFTSModel.fullTextSearch(from: db, matching: .match("zebra") && \.$category == 99, options: .default)
        XCTAssertEqual(none.count, 0)
        await db.close()
    }

    // Search on an empty table, a query matching all rows, and the limit parameter.
    func testFTSEmptyTableMatchAllAndLimit() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        _ = try await EdgeQueryFTSModel.resolveSchema(in: db)

        let emptyTable = try await EdgeQueryFTSModel.fullTextSearch(from: db, matching: .match("anything"), options: .default)
        XCTAssertEqual(emptyTable.count, 0, "search on an empty table must return no results without throwing")

        for i in 1...3 {
            try await EdgeQueryFTSModel(id: i, title: "shared token row \(i)", body: "x", category: 1).write(to: db)
        }

        let all = try await EdgeQueryFTSModel.fullTextSearch(from: db, matching: .match("shared"), options: .default)
        let allIDs = try await resultIDs(all, in: db)
        XCTAssertEqual(Set(allIDs), [1, 2, 3])

        let limited = try await EdgeQueryFTSModel.fullTextSearch(from: db, matching: .match("shared"), limit: 2, options: .default)
        XCTAssertEqual(limited.count, 2, "limit parameter must be respected")

        let limitZero = try await EdgeQueryFTSModel.fullTextSearch(from: db, matching: .match("shared"), limit: 0, options: .default)
        XCTAssertEqual(limitZero.count, 0, "limit: 0 must return no results")
        await db.close()
    }

    // Trigger integrity: the FTS index must reflect a row through write, instance update,
    // structured update, and delete.
    func testFTSIndexReflectsRowLifecycle() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await EdgeQueryFTSModel(id: 1, title: "gazelle running", body: "x", category: 1).write(to: db)

        let afterInsert = try await EdgeQueryFTSModel.fullTextSearch(from: db, matching: .match("gazelle"), options: .default)
        let afterInsertIDs = try await resultIDs(afterInsert, in: db)
        XCTAssertEqual(afterInsertIDs, [1])

        // Instance update
        var instance = try await EdgeQueryFTSModel.read(from: db, id: 1)!
        instance.title = "buffalo resting"
        try await instance.write(to: db)

        let staleAfterUpdate = try await EdgeQueryFTSModel.fullTextSearch(from: db, matching: .match("gazelle"), options: .default)
        XCTAssertEqual(staleAfterUpdate.count, 0, "old terms must leave the index on update")
        let freshAfterUpdate = try await EdgeQueryFTSModel.fullTextSearch(from: db, matching: .match("buffalo"), options: .default)
        let freshAfterUpdateIDs = try await resultIDs(freshAfterUpdate, in: db)
        XCTAssertEqual(freshAfterUpdateIDs, [1])

        // Structured update
        try await EdgeQueryFTSModel.update(in: db, set: [ \.$title : "ibex climbing" ], matching: \.$id == 1)
        let staleAfterStructured = try await EdgeQueryFTSModel.fullTextSearch(from: db, matching: .match("buffalo"), options: .default)
        XCTAssertEqual(staleAfterStructured.count, 0)
        let freshAfterStructured = try await EdgeQueryFTSModel.fullTextSearch(from: db, matching: .match("ibex"), options: .default)
        let freshAfterStructuredIDs = try await resultIDs(freshAfterStructured, in: db)
        XCTAssertEqual(freshAfterStructuredIDs, [1])

        // Delete
        try await EdgeQueryFTSModel.delete(from: db, matching: \.$id == 1)
        let afterDelete = try await EdgeQueryFTSModel.fullTextSearch(from: db, matching: .match("ibex"), options: .default)
        XCTAssertEqual(afterDelete.count, 0, "deleted rows must leave the index")
        await db.close()
    }

    // The default search options specify preloadInstances: true, so every search result
    // must carry a non-nil preloadedInstance.
    func testFTSPreloadedInstancesPopulatedByDefault() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await EdgeQueryFTSModel(id: 1, title: "kestrel hovering", body: "x", category: 1).write(to: db)

        let results = try await EdgeQueryFTSModel.fullTextSearch(from: db, matching: .match("kestrel"), options: .default)
        XCTAssertEqual(results.count, 1)

        // Regression guard: for models with an INTEGER PRIMARY KEY (rowid alias), a bare
        // "SELECT rowid, *" names the rowid column after the alias (e.g. "id"), which used
        // to make the preload lookup miss and leave preloadedInstance silently nil.
        let preloaded = results.first?.preloadedInstance
        XCTAssertNotNil(preloaded, "preloadInstances is on by default, but preloadedInstance is nil (rowid-alias column naming bug)")
        await db.close()
    }

    // bm25 ranking sanity: a term in the weight-3.0 title column must outrank
    // the same term appearing only in the weight-1.0 body column.
    func testFTSBM25ColumnWeighting() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await EdgeQueryFTSModel(id: 1, title: "falcon overhead", body: "nothing notable here", category: 1).write(to: db)
        try await EdgeQueryFTSModel(id: 2, title: "nothing notable here", body: "falcon overhead", category: 1).write(to: db)

        let results = try await EdgeQueryFTSModel.fullTextSearch(from: db, matching: .match("falcon"), options: .default)
        XCTAssertEqual(results.count, 2)
        let orderedIDs = try await resultIDs(results, in: db)
        XCTAssertEqual(orderedIDs.first, 1, "the weight-3.0 title match must rank first")

        var scoresByID: [Int: Double] = [:]
        for result in results {
            if let instance = try await result.instance(from: db) { scoresByID[instance.id] = result.score }
        }
        let titleScore = scoresByID[1] ?? -1
        let bodyScore = scoresByID[2] ?? -1
        XCTAssertGreaterThan(titleScore, bodyScore, "weight-3.0 column match must score higher than weight-1.0 column match")
        XCTAssertGreaterThan(bodyScore, 0, "scores are negated bm25 values and should be positive")
        await db.close()
    }
}
