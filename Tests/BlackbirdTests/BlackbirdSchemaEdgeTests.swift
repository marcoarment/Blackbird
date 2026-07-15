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
//  BlackbirdSchemaEdgeTests.swift
//
//  Edge-case tests for odd-but-legal schemas (SQL-keyword and unicode
//  identifiers, PK-only tables, multi-column and hostile primary keys),
//  adversarial migrations (type changes, PK changes, index changes,
//  unique-index-over-duplicates), and schema-resolution behavior.
//
//  Note: BlackbirdModel's schema validator hook (validateSchema(core:)) is
//  internal with no public per-model override, so there is intentionally no
//  "validator veto" test here.
//

import XCTest
import Combine
import SQLite3
@testable import Blackbird

// MARK: - Odd but legal schema models

struct EdgeSchemaOnlyPKModel: BlackbirdModel {
    static let tableName = "EdgeSchemaOnlyPK"
    @BlackbirdColumn var id: Int64
}

struct EdgeSchemaKeywordModel: BlackbirdModel {
    static let tableName = "EdgeSchemaKeywords"
    static let primaryKey: [BlackbirdColumnKeyPath] = [ \.$order ]
    var id: Int { order }


    @BlackbirdColumn var order: Int
    @BlackbirdColumn var select: String
    @BlackbirdColumn var group: String
    @BlackbirdColumn var index: Int
    @BlackbirdColumn var `where`: String?
}

struct EdgeSchemaKeywordIndexModel: BlackbirdModel {
    static let tableName = "EdgeSchemaKeywordIndex"
    static let indexes: [[BlackbirdColumnKeyPath]] = [
        [ \.$order ]
    ]

    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var order: Int
}

struct EdgeSchemaCaféModel: BlackbirdModel {
    static let tableName = "EdgeSchemaCafé"

    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var café: String
}

struct EdgeSchemaStringPKModel: BlackbirdModel {
    static let tableName = "EdgeSchemaStringPK"
    static let primaryKey: [BlackbirdColumnKeyPath] = [ \.$key ]
    var id: String { key }


    @BlackbirdColumn var key: String
    @BlackbirdColumn var name: String
}

struct EdgeSchemaIntPKModel: BlackbirdModel {
    static let tableName = "EdgeSchemaIntPK"

    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var name: String
}

struct EdgeSchemaMultiPKModel: BlackbirdModel {
    static let tableName = "EdgeSchemaMultiPK"
    static let primaryKey: [BlackbirdColumnKeyPath] = [ \.$groupID, \.$name ]
    var id: String { "\(groupID):\(name)" }


    @BlackbirdColumn var groupID: Int64
    @BlackbirdColumn var name: String
    @BlackbirdColumn var value: Int
}

// MARK: - Migration pairs

struct EdgeSchemaAddOptA: BlackbirdModel {
    static let tableName = "EdgeSchemaAddOpt"
    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var name: String
}

struct EdgeSchemaAddOptB: BlackbirdModel {
    static let tableName = "EdgeSchemaAddOpt"
    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var name: String
    @BlackbirdColumn var note: String?
    @BlackbirdColumn var num: Int?
}

struct EdgeSchemaAddReqA: BlackbirdModel {
    static let tableName = "EdgeSchemaAddReq"
    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var name: String
}

struct EdgeSchemaAddReqB: BlackbirdModel {
    static let tableName = "EdgeSchemaAddReq"
    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var name: String
    @BlackbirdColumn var tag: String
    @BlackbirdColumn var count: Int
}

struct EdgeSchemaDropColA: BlackbirdModel {
    static let tableName = "EdgeSchemaDropCol"
    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var keep: String
    @BlackbirdColumn var extra: Int
}

struct EdgeSchemaDropColB: BlackbirdModel {
    static let tableName = "EdgeSchemaDropCol"
    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var keep: String
}

struct EdgeSchemaAddDropA: BlackbirdModel {
    static let tableName = "EdgeSchemaAddDrop"
    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var keep: String
    @BlackbirdColumn var dropme: Int
}

struct EdgeSchemaAddDropB: BlackbirdModel {
    static let tableName = "EdgeSchemaAddDrop"
    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var keep: String
    @BlackbirdColumn var added: String?
}

struct EdgeSchemaIntToStrA: BlackbirdModel {
    static let tableName = "EdgeSchemaIntToStr"
    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var val: Int
}

struct EdgeSchemaIntToStrB: BlackbirdModel {
    static let tableName = "EdgeSchemaIntToStr"
    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var val: String // type change: forces full table rebuild
}

struct EdgeSchemaStrToIntA: BlackbirdModel {
    static let tableName = "EdgeSchemaStrToInt"
    static let primaryKey: [BlackbirdColumnKeyPath] = [ \.$key ]
    var id: String { key }

    @BlackbirdColumn var key: String
    @BlackbirdColumn var val: String
}

struct EdgeSchemaStrToIntB: BlackbirdModel {
    static let tableName = "EdgeSchemaStrToInt"
    static let primaryKey: [BlackbirdColumnKeyPath] = [ \.$key ]
    var id: String { key }

    @BlackbirdColumn var key: String
    @BlackbirdColumn var val: Int // type change: forces full table rebuild
}

struct EdgeSchemaPKExpandA: BlackbirdModel {
    static let tableName = "EdgeSchemaPKExpand"
    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var category: String
    @BlackbirdColumn var score: Int
}

struct EdgeSchemaPKExpandB: BlackbirdModel {
    static let tableName = "EdgeSchemaPKExpand"
    static let primaryKey: [BlackbirdColumnKeyPath] = [ \.$id, \.$category ]
    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var category: String
    @BlackbirdColumn var score: Int
}

struct EdgeSchemaIdxAddA: BlackbirdModel {
    static let tableName = "EdgeSchemaIdxAdd"
    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var title: String
}

struct EdgeSchemaIdxAddB: BlackbirdModel {
    static let tableName = "EdgeSchemaIdxAdd"
    static let indexes: [[BlackbirdColumnKeyPath]] = [
        [ \.$title ]
    ]
    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var title: String
}

struct EdgeSchemaIdxDropA: BlackbirdModel {
    static let tableName = "EdgeSchemaIdxDrop"
    static let indexes: [[BlackbirdColumnKeyPath]] = [
        [ \.$title ]
    ]
    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var title: String
}

struct EdgeSchemaIdxDropB: BlackbirdModel {
    static let tableName = "EdgeSchemaIdxDrop"
    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var title: String
}

struct EdgeSchemaIdxUniqueA: BlackbirdModel {
    static let tableName = "EdgeSchemaIdxUnique"
    static let indexes: [[BlackbirdColumnKeyPath]] = [
        [ \.$title ]
    ]
    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var title: String
}

struct EdgeSchemaIdxUniqueB: BlackbirdModel {
    static let tableName = "EdgeSchemaIdxUnique"
    static let uniqueIndexes: [[BlackbirdColumnKeyPath]] = [
        [ \.$title ]
    ]
    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var title: String
}

struct EdgeSchemaUniqueDupA: BlackbirdModel {
    static let tableName = "EdgeSchemaUniqueDup"
    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var slug: String
}

struct EdgeSchemaUniqueDupB: BlackbirdModel {
    static let tableName = "EdgeSchemaUniqueDup"
    static let uniqueIndexes: [[BlackbirdColumnKeyPath]] = [
        [ \.$slug ]
    ]
    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var slug: String
}

struct EdgeSchemaReorderA: BlackbirdModel {
    static let tableName = "EdgeSchemaReorder"
    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var alpha: String
    @BlackbirdColumn var beta: Int
}

struct EdgeSchemaReorderB: BlackbirdModel {
    static let tableName = "EdgeSchemaReorder"
    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var beta: Int    // same schema, columns declared in a different order
    @BlackbirdColumn var alpha: String
}

// MARK: - Schema-resolution models

struct EdgeSchemaResolveTwiceModel: BlackbirdModel {
    static let tableName = "EdgeSchemaResolveTwice"
    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var title: String
}

struct EdgeSchemaValidatedModel: BlackbirdModel {
    static let tableName = "EdgeSchemaValidated"
    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var title: String
}

struct EdgeSchemaTwoDBModel: BlackbirdModel {
    static let tableName = "EdgeSchemaTwoDB"
    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var title: String
}

// MARK: - Tests

final class BlackbirdSchemaEdgeTests: XCTestCase, @unchecked Sendable {
    var sqliteFilename = ""
    var extraSqliteFilename = ""

    override func setUpWithError() throws {
        let dir = FileManager.default.temporaryDirectory.path
        sqliteFilename = "\(dir)/test\(Int64.random(in: 0..<Int64.max)).sqlite"
        extraSqliteFilename = "\(dir)/test\(Int64.random(in: 0..<Int64.max)).sqlite"
    }

    override func tearDownWithError() throws {
        for filename in [sqliteFilename, extraSqliteFilename] {
            if filename != "", filename != ":memory:", FileManager.default.fileExists(atPath: filename) {
                for path in Blackbird.Database.allFilePaths(for: filename) {
                    try? FileManager.default.removeItem(atPath: path)
                }
            }
        }
    }

    // MARK: Odd but legal schemas

    // A model whose only column is the primary key.
    func testOnlyPrimaryKeyColumnModel() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)

        try await EdgeSchemaOnlyPKModel(id: 7).write(to: db)
        try await EdgeSchemaOnlyPKModel(id: 8).write(to: db)

        let read7 = try await EdgeSchemaOnlyPKModel.read(from: db, id: 7)
        XCTAssertEqual(read7?.id, 7)

        // Re-writing an identical, already-existing row must be a no-op upsert, not an error.
        // (A PK-only table has an empty upsert clause, leaving a bare INSERT.)
        do {
            try await EdgeSchemaOnlyPKModel(id: 7).write(to: db)
        } catch {
            XCTFail("Regression: re-writing an existing row of a PK-only model must upsert as a no-op, but threw: \(error)")
        }

        let all = try await EdgeSchemaOnlyPKModel.read(from: db, matching: .all)
        XCTAssertEqual(Set(all.map(\.id)), [7, 8])

        try await EdgeSchemaOnlyPKModel(id: 7).delete(from: db)
        let remaining = try await EdgeSchemaOnlyPKModel.read(from: db, matching: .all)
        XCTAssertEqual(remaining.map(\.id), [8])
        await db.close()
    }

    // Column names that are SQL keywords, including a SQL-keyword primary key:
    // full write/read/update(matching:)/delete cycle.
    func testKeywordColumnsFullCycle() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)

        try await EdgeSchemaKeywordModel(order: 1, select: "s1", group: "g1", index: 10, where: "w1").write(to: db)
        try await EdgeSchemaKeywordModel(order: 2, select: "s2", group: "g1", index: 20, where: nil).write(to: db)
        try await EdgeSchemaKeywordModel(order: 3, select: "s3", group: "g2", index: 30, where: "w3").write(to: db)

        // read by (keyword-named) primary key
        let row1 = try await EdgeSchemaKeywordModel.read(from: db, primaryKey: 1)
        XCTAssertEqual(row1?.select, "s1")
        XCTAssertEqual(row1?.`where`, "w1")

        // sqlWhere reads against keyword columns
        let bySelect = try await EdgeSchemaKeywordModel.read(from: db, sqlWhere: "`select` = ?", "s2")
        XCTAssertEqual(bySelect.map(\.order), [2])
        let nullWhere = try await EdgeSchemaKeywordModel.read(from: db, sqlWhere: "`where` IS NULL")
        XCTAssertEqual(nullWhere.map(\.order), [2])

        // structured queries: matching + orderBy on keyword columns
        let g1Desc = try await EdgeSchemaKeywordModel.read(from: db, matching: \.$group == "g1", orderBy: .descending(\.$order))
        XCTAssertEqual(g1Desc.map(\.order), [2, 1])
        let cmp = try await EdgeSchemaKeywordModel.read(from: db, matching: \.$index > 15 && \.$group == "g1")
        XCTAssertEqual(cmp.map(\.order), [2])

        // structured update on keyword columns
        try await EdgeSchemaKeywordModel.update(in: db, set: [ \.$select : "updated", \.$where : "w2" ], matching: \.$order == 2)
        let updated = try await EdgeSchemaKeywordModel.read(from: db, primaryKey: 2)
        XCTAssertEqual(updated?.select, "updated")
        XCTAssertEqual(updated?.`where`, "w2")

        // instance write (upsert) and delete through the keyword PK
        var modified = try await EdgeSchemaKeywordModel.read(from: db, primaryKey: 3)!
        modified.group = "g3"
        try await modified.write(to: db)
        let rewritten = try await EdgeSchemaKeywordModel.read(from: db, primaryKey: 3)
        XCTAssertEqual(rewritten?.group, "g3")

        try await rewritten!.delete(from: db)
        let count = try await EdgeSchemaKeywordModel.count(in: db, matching: .all)
        XCTAssertEqual(count, 2)
        await db.close()
    }

    // read(from:primaryKeys:) builds "WHERE <pkName> IN (...)" without quoting
    // the primary-key column name (BlackbirdModel.swift, read(from:primaryKeys:)),
    // which is a syntax error when the PK is a SQL keyword like `order`.
    func testKeywordPrimaryKeyReadByPrimaryKeys() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await EdgeSchemaKeywordModel(order: 1, select: "a", group: "g", index: 1, where: nil).write(to: db)
        try await EdgeSchemaKeywordModel(order: 2, select: "b", group: "g", index: 2, where: nil).write(to: db)

        do {
            let rows = try await EdgeSchemaKeywordModel.read(from: db, primaryKeys: [1, 2])
            XCTAssertEqual(Set(rows.map(\.order)), [1, 2])
        } catch {
            XCTFail("Regression: read(from:primaryKeys:) must backtick-quote the primary-key column name; a SQL-keyword PK failed: \(error)")
        }
        await db.close()
    }

    // Index definitions (Blackbird.Index.definition(tableName:)) don't quote
    // column names, so an index on a SQL-keyword column produces invalid SQL.
    func testIndexOnKeywordColumn() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        do {
            _ = try await EdgeSchemaKeywordIndexModel.resolveSchema(in: db)
            try await EdgeSchemaKeywordIndexModel(id: 1, order: 5).write(to: db)
            let row = try await EdgeSchemaKeywordIndexModel.read(from: db, id: 1)
            XCTAssertEqual(row?.order, 5)
        } catch {
            XCTFail("Regression: Blackbird.Index.definition(tableName:) must backtick-quote table and column names; an index on a SQL-keyword column failed to create: \(error)")
        }
        await db.close()
    }

    // Unicode table name and unicode column name: write/read round-trip.
    func testUnicodeTableAndColumnNames() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)

        try await EdgeSchemaCaféModel(id: 1, café: "crème brûlée ☕️ 日本語").write(to: db)
        let row = try await EdgeSchemaCaféModel.read(from: db, id: 1)
        XCTAssertEqual(row?.café, "crème brûlée ☕️ 日本語")

        let bySqlWhere = try await EdgeSchemaCaféModel.read(from: db, sqlWhere: "`café` = ?", "crème brûlée ☕️ 日本語")
        XCTAssertEqual(bySqlWhere.map(\.id), [1])

        let byMatching = try await EdgeSchemaCaféModel.read(from: db, matching: \.$café == "crème brûlée ☕️ 日本語")
        XCTAssertEqual(byMatching.map(\.id), [1])

        try await EdgeSchemaCaféModel.update(in: db, set: [ \.$café : "更新済み" ], matching: \.$id == 1)
        let updated = try await EdgeSchemaCaféModel.read(from: db, id: 1)
        XCTAssertEqual(updated?.café, "更新済み")
        await db.close()
    }

    // String primary keys containing quotes/unicode/SQL-injection bait, and an empty-string primary key.
    func testStringPrimaryKeyWithQuotesUnicodeAndEmpty() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        let hostileKey = "O'Brien \"quotes\" `ticks` café 日本語; DROP TABLE EdgeSchemaStringPK; --"

        try await EdgeSchemaStringPKModel(key: hostileKey, name: "hostile").write(to: db)
        try await EdgeSchemaStringPKModel(key: "", name: "empty").write(to: db)

        let hostile = try await EdgeSchemaStringPKModel.read(from: db, primaryKey: hostileKey)
        XCTAssertEqual(hostile?.name, "hostile")

        let empty = try await EdgeSchemaStringPKModel.read(from: db, primaryKey: "")
        XCTAssertEqual(empty?.name, "empty")

        // update the empty-key row through an instance write (upsert path)
        var emptyRow = empty!
        emptyRow.name = "empty updated"
        try await emptyRow.write(to: db)
        let emptyAfter = try await EdgeSchemaStringPKModel.read(from: db, primaryKey: "")
        XCTAssertEqual(emptyAfter?.name, "empty updated")

        try await hostile!.delete(from: db)
        let all = try await EdgeSchemaStringPKModel.read(from: db, matching: .all)
        XCTAssertEqual(all.map(\.key), [""])
        await db.close()
    }

    // Negative, zero, and Int64.min primary keys (rowid aliases allow all of these).
    func testNegativeAndZeroIntPrimaryKeys() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)

        try await EdgeSchemaIntPKModel(id: -5, name: "negative").write(to: db)
        try await EdgeSchemaIntPKModel(id: 0, name: "zero").write(to: db)
        try await EdgeSchemaIntPKModel(id: Int64.min, name: "min").write(to: db)

        let negative = try await EdgeSchemaIntPKModel.read(from: db, id: -5)
        XCTAssertEqual(negative?.name, "negative")
        let zero = try await EdgeSchemaIntPKModel.read(from: db, id: 0)
        XCTAssertEqual(zero?.name, "zero")
        let min = try await EdgeSchemaIntPKModel.read(from: db, id: Int64.min)
        XCTAssertEqual(min?.name, "min")

        try await zero!.delete(from: db)
        let ids = Set(try await EdgeSchemaIntPKModel.read(from: db, matching: .all).map(\.id))
        XCTAssertEqual(ids, [-5, Int64.min])
        await db.close()
    }

    // Multi-column primary key: write/read(multicolumnPrimaryKey:)/delete,
    // including two rows sharing the first PK component.
    func testMultiColumnPrimaryKeyCRUD() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)

        try await EdgeSchemaMultiPKModel(groupID: 1, name: "a", value: 1).write(to: db)
        try await EdgeSchemaMultiPKModel(groupID: 1, name: "b", value: 2).write(to: db)
        try await EdgeSchemaMultiPKModel(groupID: 2, name: "a", value: 3).write(to: db)

        let oneB = try await EdgeSchemaMultiPKModel.read(from: db, multicolumnPrimaryKey: [1, "b"])
        XCTAssertEqual(oneB?.value, 2)
        let twoA = try await EdgeSchemaMultiPKModel.read(from: db, multicolumnPrimaryKey: ["groupID": 2, "name": "a"])
        XCTAssertEqual(twoA?.value, 3)

        // upsert on the full compound key: must update, not insert
        try await EdgeSchemaMultiPKModel(groupID: 1, name: "a", value: 100).write(to: db)
        let count = try await EdgeSchemaMultiPKModel.count(in: db, matching: .all)
        XCTAssertEqual(count, 3)
        let oneA = try await EdgeSchemaMultiPKModel.read(from: db, multicolumnPrimaryKey: [1, "a"])
        XCTAssertEqual(oneA?.value, 100)

        try await oneA!.delete(from: db)
        let afterDelete = try await EdgeSchemaMultiPKModel.read(from: db, multicolumnPrimaryKey: [1, "a"])
        XCTAssertNil(afterDelete)
        let oneBStill = try await EdgeSchemaMultiPKModel.read(from: db, multicolumnPrimaryKey: [1, "b"])
        XCTAssertNotNil(oneBStill, "deleting (1, a) must not affect (1, b), which shares the first PK component")
        await db.close()
    }

    // changePublisher filtered by multicolumnPrimaryKey must only deliver
    // keyed changes for that exact compound key.
    func testMultiColumnPrimaryKeyChangePublisherFiltering() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)

        try await EdgeSchemaMultiPKModel(groupID: 1, name: "a", value: 1).write(to: db)
        try await EdgeSchemaMultiPKModel(groupID: 1, name: "b", value: 2).write(to: db)
        try await Task.sleep(nanoseconds: 300_000_000) // let the setup writes' change reports flush

        let received = Blackbird.Locked<[Blackbird.PrimaryKeyValues?]>([])
        let expectation = expectation(description: "filtered change received")
        expectation.assertForOverFulfill = false
        let subscription = EdgeSchemaMultiPKModel.changePublisher(in: db, multicolumnPrimaryKey: [1, "a"] as [Sendable]).sink { change in
            received.value.append(change.changedPrimaryKeys)
            expectation.fulfill()
        }
        defer { subscription.cancel() }

        // Change to (1, "b"): must NOT be delivered. Then a change to (1, "a"): must be delivered.
        try await EdgeSchemaMultiPKModel.update(in: db, set: [ \.$value : 20 ], matching: \.$groupID == 1 && \.$name == "b")
        try await EdgeSchemaMultiPKModel.update(in: db, set: [ \.$value : 10 ], matching: \.$groupID == 1 && \.$name == "a")

        await fulfillment(of: [expectation], timeout: 5)
        try await Task.sleep(nanoseconds: 300_000_000) // catch any straggler (wrongly unfiltered) events

        let changes = received.value
        XCTAssertFalse(changes.isEmpty)
        for changedKeys in changes {
            guard let changedKeys else { continue } // whole-table changes legitimately pass the filter
            XCTAssert(changedKeys.contains([.integer(1), .text("a")]), "received a change that doesn't include the watched key: \(changedKeys)")
            XCTAssertFalse(changedKeys == [[.integer(1), .text("b")]], "received a change keyed solely to a different row")
        }
        await db.close()
    }

    // MARK: Migrations

    // Adding optional columns: existing rows read as nil.
    func testMigrationAddOptionalColumns() async throws {
        var db = try Blackbird.Database(path: sqliteFilename)
        try await EdgeSchemaAddOptA(id: 1, name: "one").write(to: db)
        try await EdgeSchemaAddOptA(id: 2, name: "two").write(to: db)
        await db.close()

        db = try Blackbird.Database(path: sqliteFilename)
        let resolution = try await EdgeSchemaAddOptB.resolveSchema(in: db)
        XCTAssert(resolution.contains(.migratedTable))

        let row1 = try await EdgeSchemaAddOptB.read(from: db, id: 1)
        XCTAssertEqual(row1?.name, "one")
        XCTAssertNil(row1?.note ?? nil)
        XCTAssertNil(row1?.num ?? nil)
        await db.close()

        // reopen: the migration must have been committed durably
        db = try Blackbird.Database(path: sqliteFilename)
        let resolution2 = try await EdgeSchemaAddOptB.resolveSchema(in: db)
        XCTAssertFalse(resolution2.contains(.migratedTable), "migration did not persist across reopen")
        let all = try await EdgeSchemaAddOptB.read(from: db, matching: .all)
        XCTAssertEqual(all.count, 2)
        await db.close()
    }

    // Adding NON-optional String/Int columns: existing rows get default values ("" / 0).
    func testMigrationAddNonOptionalColumns() async throws {
        var db = try Blackbird.Database(path: sqliteFilename)
        try await EdgeSchemaAddReqA(id: 1, name: "one").write(to: db)
        await db.close()

        db = try Blackbird.Database(path: sqliteFilename)
        let resolution = try await EdgeSchemaAddReqB.resolveSchema(in: db)
        XCTAssert(resolution.contains(.migratedTable))

        let row = try await EdgeSchemaAddReqB.read(from: db, id: 1)
        XCTAssertEqual(row?.name, "one")
        XCTAssertEqual(row?.tag, "")
        XCTAssertEqual(row?.count, 0)
        await db.close()

        db = try Blackbird.Database(path: sqliteFilename)
        let resolution2 = try await EdgeSchemaAddReqB.resolveSchema(in: db)
        XCTAssertFalse(resolution2.contains(.migratedTable), "migration did not persist across reopen")
        await db.close()
    }

    // Dropping a column: remaining data intact.
    func testMigrationDropColumn() async throws {
        var db = try Blackbird.Database(path: sqliteFilename)
        try await EdgeSchemaDropColA(id: 1, keep: "kept one", extra: 11).write(to: db)
        try await EdgeSchemaDropColA(id: 2, keep: "kept two", extra: 22).write(to: db)
        await db.close()

        db = try Blackbird.Database(path: sqliteFilename)
        let resolution = try await EdgeSchemaDropColB.resolveSchema(in: db)
        XCTAssert(resolution.contains(.migratedTable))

        let all = try await EdgeSchemaDropColB.read(from: db, matching: .all, orderBy: .ascending(\.$id))
        XCTAssertEqual(all.map(\.keep), ["kept one", "kept two"])
        await db.close()

        db = try Blackbird.Database(path: sqliteFilename)
        let resolution2 = try await EdgeSchemaDropColB.resolveSchema(in: db)
        XCTAssertFalse(resolution2.contains(.migratedTable), "migration did not persist across reopen")
        await db.close()
    }

    // Add and drop columns in a single migration.
    func testMigrationAddAndDropColumns() async throws {
        var db = try Blackbird.Database(path: sqliteFilename)
        try await EdgeSchemaAddDropA(id: 1, keep: "still here", dropme: 99).write(to: db)
        await db.close()

        db = try Blackbird.Database(path: sqliteFilename)
        let resolution = try await EdgeSchemaAddDropB.resolveSchema(in: db)
        XCTAssert(resolution.contains(.migratedTable))

        let row = try await EdgeSchemaAddDropB.read(from: db, id: 1)
        XCTAssertEqual(row?.keep, "still here")
        XCTAssertNil(row?.added ?? nil)
        await db.close()

        db = try Blackbird.Database(path: sqliteFilename)
        let resolution2 = try await EdgeSchemaAddDropB.resolveSchema(in: db)
        XCTAssertFalse(resolution2.contains(.migratedTable), "migration did not persist across reopen")
        await db.close()
    }

    // Column type change Int -> String (full table rebuild) with rowid gaps:
    // data coerced per SQLite affinity, PK mapping intact.
    func testMigrationIntToStringColumn() async throws {
        var db = try Blackbird.Database(path: sqliteFilename)
        for i in 1...5 {
            try await EdgeSchemaIntToStrA(id: Int64(i), val: i * 10).write(to: db)
        }
        // rowid gaps
        try await EdgeSchemaIntToStrA.read(from: db, id: 2)?.delete(from: db)
        try await EdgeSchemaIntToStrA.read(from: db, id: 3)?.delete(from: db)
        await db.close()

        db = try Blackbird.Database(path: sqliteFilename)
        let resolution = try await EdgeSchemaIntToStrB.resolveSchema(in: db)
        XCTAssert(resolution.contains(.migratedTable))

        let all = try await EdgeSchemaIntToStrB.read(from: db, matching: .all, orderBy: .ascending(\.$id))
        XCTAssertEqual(all.map(\.id), [1, 4, 5])
        XCTAssertEqual(all.map(\.val), ["10", "40", "50"], "TEXT affinity should coerce stored integers to their string forms")
        await db.close()

        db = try Blackbird.Database(path: sqliteFilename)
        let resolution2 = try await EdgeSchemaIntToStrB.resolveSchema(in: db)
        XCTAssertFalse(resolution2.contains(.migratedTable), "rebuild did not persist across reopen")
        let survivor = try await EdgeSchemaIntToStrB.read(from: db, id: 4)
        XCTAssertEqual(survivor?.val, "40")
        await db.close()
    }

    // Column type change String -> Int (full table rebuild) on a String-PK table
    // with rowid gaps: rows survive, PK mapping intact.
    func testMigrationStringToIntColumnWithRowidGaps() async throws {
        var db = try Blackbird.Database(path: sqliteFilename)
        for i in 1...5 {
            try await EdgeSchemaStrToIntA(key: "k\(i)", val: "\(i * 10)").write(to: db)
        }
        // rowid gaps
        try await EdgeSchemaStrToIntA.read(from: db, primaryKey: "k2")?.delete(from: db)
        try await EdgeSchemaStrToIntA.read(from: db, primaryKey: "k3")?.delete(from: db)
        await db.close()

        db = try Blackbird.Database(path: sqliteFilename)
        let resolution = try await EdgeSchemaStrToIntB.resolveSchema(in: db)
        XCTAssert(resolution.contains(.migratedTable))

        let all = try await EdgeSchemaStrToIntB.read(from: db, matching: .all, orderBy: .ascending(\.$key))
        XCTAssertEqual(all.map(\.key), ["k1", "k4", "k5"])
        XCTAssertEqual(all.map(\.val), [10, 40, 50], "INTEGER affinity should coerce numeric strings to integers")

        let k4 = try await EdgeSchemaStrToIntB.read(from: db, primaryKey: "k4")
        XCTAssertEqual(k4?.val, 40, "primary-key mapping broken after rebuild with rowid gaps")
        await db.close()

        db = try Blackbird.Database(path: sqliteFilename)
        let resolution2 = try await EdgeSchemaStrToIntB.resolveSchema(in: db)
        XCTAssertFalse(resolution2.contains(.migratedTable), "rebuild did not persist across reopen")
        await db.close()
    }

    // Primary-key change: single-column id -> multi-column (id, category), a full rebuild.
    func testMigrationSingleToMultiColumnPrimaryKey() async throws {
        var db = try Blackbird.Database(path: sqliteFilename)
        try await EdgeSchemaPKExpandA(id: 1, category: "x", score: 5).write(to: db)
        try await EdgeSchemaPKExpandA(id: 2, category: "y", score: 6).write(to: db)
        await db.close()

        db = try Blackbird.Database(path: sqliteFilename)
        let resolution = try await EdgeSchemaPKExpandB.resolveSchema(in: db)
        XCTAssert(resolution.contains(.migratedTable))

        let oneX = try await EdgeSchemaPKExpandB.read(from: db, multicolumnPrimaryKey: [1, "x"])
        XCTAssertEqual(oneX?.score, 5)
        let twoY = try await EdgeSchemaPKExpandB.read(from: db, multicolumnPrimaryKey: [2, "y"])
        XCTAssertEqual(twoY?.score, 6)

        // The expanded key must now permit two rows sharing the old single-column key
        try await EdgeSchemaPKExpandB(id: 1, category: "z", score: 9).write(to: db)
        let count = try await EdgeSchemaPKExpandB.count(in: db, matching: .all)
        XCTAssertEqual(count, 3)
        await db.close()

        db = try Blackbird.Database(path: sqliteFilename)
        let resolution2 = try await EdgeSchemaPKExpandB.resolveSchema(in: db)
        XCTAssertFalse(resolution2.contains(.migratedTable), "PK migration did not persist across reopen")
        await db.close()
    }

    // Index added.
    func testMigrationAddIndex() async throws {
        var db = try Blackbird.Database(path: sqliteFilename)
        try await EdgeSchemaIdxAddA(id: 1, title: "one").write(to: db)
        await db.close()

        db = try Blackbird.Database(path: sqliteFilename)
        let resolution = try await EdgeSchemaIdxAddB.resolveSchema(in: db)
        XCTAssert(resolution.contains(.migratedTable))

        let indexRows = try await db.query("SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'EdgeSchemaIdxAdd'")
        let indexNames = indexRows.compactMap { $0["name"]?.stringValue }
        XCTAssert(indexNames.contains("EdgeSchemaIdxAdd+index+title"), "index was not created: \(indexNames)")

        let row = try await EdgeSchemaIdxAddB.read(from: db, id: 1)
        XCTAssertEqual(row?.title, "one")
        await db.close()

        db = try Blackbird.Database(path: sqliteFilename)
        let resolution2 = try await EdgeSchemaIdxAddB.resolveSchema(in: db)
        XCTAssertFalse(resolution2.contains(.migratedTable), "index migration did not persist across reopen")
        await db.close()
    }

    // Index removed.
    func testMigrationRemoveIndex() async throws {
        var db = try Blackbird.Database(path: sqliteFilename)
        try await EdgeSchemaIdxDropA(id: 1, title: "one").write(to: db)
        await db.close()

        db = try Blackbird.Database(path: sqliteFilename)
        let resolution = try await EdgeSchemaIdxDropB.resolveSchema(in: db)
        XCTAssert(resolution.contains(.migratedTable))

        let indexRows = try await db.query("SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'EdgeSchemaIdxDrop' AND name LIKE '%+index+%'")
        let indexNames = indexRows.compactMap { $0["name"]?.stringValue }
        XCTAssert(indexNames.isEmpty, "index was not dropped: \(indexNames)")

        let row = try await EdgeSchemaIdxDropB.read(from: db, id: 1)
        XCTAssertEqual(row?.title, "one")
        await db.close()

        db = try Blackbird.Database(path: sqliteFilename)
        let resolution2 = try await EdgeSchemaIdxDropB.resolveSchema(in: db)
        XCTAssertFalse(resolution2.contains(.migratedTable), "index removal did not persist across reopen")
        await db.close()
    }

    // Index changed to unique (on non-duplicate data).
    func testMigrationIndexToUnique() async throws {
        var db = try Blackbird.Database(path: sqliteFilename)
        try await EdgeSchemaIdxUniqueA(id: 1, title: "one").write(to: db)
        try await EdgeSchemaIdxUniqueA(id: 2, title: "two").write(to: db)
        await db.close()

        db = try Blackbird.Database(path: sqliteFilename)
        let resolution = try await EdgeSchemaIdxUniqueB.resolveSchema(in: db)
        XCTAssert(resolution.contains(.migratedTable))

        let sqlRows = try await db.query("SELECT sql FROM sqlite_master WHERE type = 'index' AND tbl_name = 'EdgeSchemaIdxUnique' AND name = 'EdgeSchemaIdxUnique+index+title'")
        let indexSQL = sqlRows.first?["sql"]?.stringValue ?? ""
        XCTAssert(indexSQL.localizedCaseInsensitiveContains("UNIQUE"), "index was not recreated as UNIQUE: \(indexSQL)")

        // uniqueness must now be enforced
        do {
            try await EdgeSchemaIdxUniqueB(id: 3, title: "one").write(to: db)
            XCTFail("expected unique-constraint failure after index became unique")
        } catch Blackbird.Database.Error.uniqueConstraintFailed { } // expected

        let count = try await EdgeSchemaIdxUniqueB.count(in: db, matching: .all)
        XCTAssertEqual(count, 2)
        await db.close()

        db = try Blackbird.Database(path: sqliteFilename)
        let resolution2 = try await EdgeSchemaIdxUniqueB.resolveSchema(in: db)
        XCTAssertFalse(resolution2.contains(.migratedTable), "unique-index migration did not persist across reopen")
        await db.close()
    }

    // Adding a UNIQUE index when existing data contains duplicates: resolveSchema
    // throwing is acceptable; silently dropping rows is not. Documents actual behavior.
    func testMigrationAddUniqueIndexWithDuplicateData() async throws {
        var db = try Blackbird.Database(path: sqliteFilename)
        try await EdgeSchemaUniqueDupA(id: 1, slug: "same").write(to: db)
        try await EdgeSchemaUniqueDupA(id: 2, slug: "same").write(to: db)
        try await EdgeSchemaUniqueDupA(id: 3, slug: "other").write(to: db)
        await db.close()

        db = try Blackbird.Database(path: sqliteFilename)
        var migrationThrew = false
        do {
            let resolution = try await EdgeSchemaUniqueDupB.resolveSchema(in: db)
            // If it didn't throw, SQLite somehow created a unique index over duplicates -- inspect below.
            _ = resolution
        } catch {
            // Actual observed behavior: resolveSchema throws (CREATE UNIQUE INDEX fails on the
            // duplicate data and the migration transaction rolls back). This is acceptable.
            migrationThrew = true
        }

        // Whether or not the migration threw, no rows may be silently lost.
        // (Model-typed reads through EdgeSchemaUniqueDupB would re-attempt the migration, so query raw.)
        let countRows = try await db.query("SELECT COUNT(*) AS c FROM EdgeSchemaUniqueDup")
        let rowCount = countRows.first?["c"]?.intValue
        XCTAssertEqual(rowCount, 3, "adding a UNIQUE index over duplicate data silently dropped rows")

        if !migrationThrew {
            // If resolution succeeded, the duplicates must still all be present AND the index must exist.
            let dupRows = try await db.query("SELECT COUNT(*) AS c FROM EdgeSchemaUniqueDup WHERE slug = 'same'")
            let dupCount = dupRows.first?["c"]?.intValue
            XCTAssertEqual(dupCount, 2, "duplicate rows were dropped to satisfy the new UNIQUE index")
        }
        await db.close()

        // Reopen: data must still be intact on disk.
        db = try Blackbird.Database(path: sqliteFilename)
        let countRows2 = try await db.query("SELECT COUNT(*) AS c FROM EdgeSchemaUniqueDup")
        let rowCount2 = countRows2.first?["c"]?.intValue
        XCTAssertEqual(rowCount2, 3, "row loss after reopen following failed unique-index migration")
        await db.close()
    }

    // Identical schema with columns declared in a different order must NOT
    // report .migratedTable (or recreate the table).
    func testMigrationIdenticalSchemaDifferentColumnOrder() async throws {
        var db = try Blackbird.Database(path: sqliteFilename)
        try await EdgeSchemaReorderA(id: 1, alpha: "a", beta: 2).write(to: db)
        await db.close()

        db = try Blackbird.Database(path: sqliteFilename)
        let resolution = try await EdgeSchemaReorderB.resolveSchema(in: db)
        XCTAssertFalse(resolution.contains(.migratedTable), "identical schema (different column declaration order) was treated as a migration")
        XCTAssertFalse(resolution.contains(.createdTable))

        let row = try await EdgeSchemaReorderB.read(from: db, id: 1)
        XCTAssertEqual(row?.alpha, "a")
        XCTAssertEqual(row?.beta, 2)
        await db.close()
    }

    // MARK: Schema-resolution behavior

    // resolveSchema twice on the same database: second call returns an empty resolution.
    func testResolveSchemaTwice() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)

        let first = try await EdgeSchemaResolveTwiceModel.resolveSchema(in: db)
        XCTAssert(first.contains(.createdTable))

        let second = try await EdgeSchemaResolveTwiceModel.resolveSchema(in: db)
        XCTAssert(second.isEmpty, "second resolveSchema on the same database should be a no-op, got \(second.rawValue)")

        try await EdgeSchemaResolveTwiceModel(id: 1, title: "t").write(to: db)
        let row = try await EdgeSchemaResolveTwiceModel.read(from: db, id: 1)
        XCTAssertEqual(row?.title, "t")
        await db.close()
    }

    // .requireModelSchemaValidationBeforeUse: with resolveSchema called first, everything works.
    // (Skipping resolveSchema fatalErrors by design, so the violation isn't testable here.)
    func testRequireSchemaValidationWithResolveWorks() async throws {
        let db = try Blackbird.Database(path: sqliteFilename, options: [.requireModelSchemaValidationBeforeUse])

        let resolution = try await EdgeSchemaValidatedModel.resolveSchema(in: db)
        XCTAssert(resolution.contains(.createdTable))

        try await EdgeSchemaValidatedModel(id: 1, title: "validated").write(to: db)
        let row = try await EdgeSchemaValidatedModel.read(from: db, id: 1)
        XCTAssertEqual(row?.title, "validated")

        try await EdgeSchemaValidatedModel.update(in: db, set: [ \.$title : "updated" ], matching: \.$id == 1)
        let updated = try await EdgeSchemaValidatedModel.read(from: db, id: 1)
        XCTAssertEqual(updated?.title, "updated")
        await db.close()
    }

    // The same model type used through two Database instances (different files)
    // in one process: schemas resolve independently with no cross-contamination.
    func testTwoDatabasesIndependentSchemas() async throws {
        let db1 = try Blackbird.Database(path: sqliteFilename)
        let db2 = try Blackbird.Database(path: extraSqliteFilename)

        let res1 = try await EdgeSchemaTwoDBModel.resolveSchema(in: db1)
        XCTAssert(res1.contains(.createdTable))
        let res2 = try await EdgeSchemaTwoDBModel.resolveSchema(in: db2)
        XCTAssert(res2.contains(.createdTable), "second database's table creation was skipped: schema resolution leaked across Database instances")

        try await EdgeSchemaTwoDBModel(id: 1, title: "in db1").write(to: db1)
        try await EdgeSchemaTwoDBModel(id: 2, title: "in db2").write(to: db2)

        let all1 = try await EdgeSchemaTwoDBModel.read(from: db1, matching: .all)
        let all2 = try await EdgeSchemaTwoDBModel.read(from: db2, matching: .all)
        XCTAssertEqual(all1.map(\.id), [1])
        XCTAssertEqual(all2.map(\.id), [2])
        XCTAssertEqual(all1.first?.title, "in db1")
        XCTAssertEqual(all2.first?.title, "in db2")

        await db1.close()
        await db2.close()
    }
}
