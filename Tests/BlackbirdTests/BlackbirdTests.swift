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
//  BlackbirdTests.swift
//  Created by Marco Arment on 11/20/22.
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
import Combine
@testable import Blackbird

func AssertNoThrowAsync(_ action: @autoclosure (() async throws -> Void)) async {
    do {
        try await action()
    } catch {
        XCTAssert(false, "Call threw error: \(error)")
    }
}

func AssertThrowsErrorAsync(_ action: @autoclosure (() async throws -> Void)) async {
    do {
        try await action()
        XCTAssert(false, "Call was expected to throw")
    } catch { }
}

final class BlackbirdTestTests: XCTestCase, @unchecked Sendable {
    enum Error: Swift.Error {
        case testError
    }

    var sqliteFilename = ""

    override func setUpWithError() throws {
        let dir = FileManager.default.temporaryDirectory.path
        sqliteFilename = "\(dir)/test\(Int64.random(in: 0..<Int64.max)).sqlite"
    }

    override func tearDownWithError() throws {
        if sqliteFilename != "", sqliteFilename != ":memory:", FileManager.default.fileExists(atPath: sqliteFilename) { try FileManager.default.removeItem(atPath: sqliteFilename) }
    }

    // Use XCTAssert and related functions to verify your tests produce the correct results.
    // Any test you write for XCTest can be annotated as throws and async.
    // Mark your test throws to produce an unexpected failure when your test encounters an uncaught error.
    // Mark your test async to allow awaiting for asynchronous code to complete. Check the results with assertions afterwards.

    func testValueConversions() throws {
        guard let n = Blackbird.Value.fromSQLiteLiteral("NULL") else { throw Error.testError }
        XCTAssert(n == .null)
        XCTAssert(n.intValue == nil)
        XCTAssert(n.doubleValue == nil)
        XCTAssert(n.stringValue == nil)
        XCTAssert(n.dataValue == nil)
        XCTAssert((try Blackbird.Value.fromAny(nil)) == n)
        XCTAssert((try Blackbird.Value.fromAny(NSNull())) == n)

        guard let i = Blackbird.Value.fromSQLiteLiteral("123456") else { throw Error.testError }
        XCTAssert(i == .integer(123456))
        XCTAssert(i.intValue == 123456)
        XCTAssert(i.doubleValue == 123456.0)
        XCTAssert(i.stringValue == "123456")
        XCTAssert(i.dataValue == "123456".data(using: .utf8))
        XCTAssert((try Blackbird.Value.fromAny(123456)) == i)
        XCTAssert((try Blackbird.Value.fromAny(Int(123456))) == i)
        XCTAssert((try Blackbird.Value.fromAny(Int8(123))) == .integer(123))
        XCTAssert((try Blackbird.Value.fromAny(Int16(12345))) == .integer(12345))
        XCTAssert((try Blackbird.Value.fromAny(Int32(123456))) == i)
        XCTAssert((try Blackbird.Value.fromAny(Int64(123456))) == i)
        XCTAssert((try Blackbird.Value.fromAny(UInt8(123))) == .integer(123))
        XCTAssert((try Blackbird.Value.fromAny(UInt16(12345))) == .integer(12345))
        XCTAssert((try Blackbird.Value.fromAny(UInt32(123456))) == i)
        XCTAssertThrowsError(try Blackbird.Value.fromAny(UInt(123456)))
        XCTAssertThrowsError(try Blackbird.Value.fromAny(UInt64(123456)))
        XCTAssert((try Blackbird.Value.fromAny(false)) == .integer(0))
        XCTAssert((try Blackbird.Value.fromAny(true)) == .integer(1))

        guard let d = Blackbird.Value.fromSQLiteLiteral("123456.789") else { throw Error.testError }
        XCTAssert(d == .double(123456.789))
        XCTAssert(d.intValue == 123456)
        XCTAssert(d.doubleValue == 123456.789)
        XCTAssert(d.stringValue == "123456.789")
        XCTAssert(d.dataValue == "123456.789".data(using: .utf8))
        XCTAssert((try Blackbird.Value.fromAny(123456.789)) == d)
        XCTAssert((try Blackbird.Value.fromAny(Float(123456.789))) == .double(123456.7890625))
        XCTAssert((try Blackbird.Value.fromAny(Double(123456.789))) == d)

        guard let s = Blackbird.Value.fromSQLiteLiteral("'abc\"🌊\"d''éƒ'''") else { throw Error.testError }
        XCTAssert(s == .text("abc\"🌊\"d'éƒ'"))
        XCTAssert(s.intValue == nil)
        XCTAssert(s.doubleValue == nil)
        XCTAssert(s.stringValue == "abc\"🌊\"d'éƒ'")
        XCTAssert(s.dataValue == "abc\"🌊\"d'éƒ'".data(using: .utf8)!)
        XCTAssert((try Blackbird.Value.fromAny("abc\"🌊\"d'éƒ'")) == s)
    
        guard let b = Blackbird.Value.fromSQLiteLiteral("X\'616263F09F8C8A64C3A9C692\'") else { throw Error.testError }
        XCTAssert(b == .data("abc🌊déƒ".data(using: .utf8)!))
        XCTAssert(b.intValue == nil)
        XCTAssert(b.doubleValue == nil)
        XCTAssert(b.stringValue == "abc🌊déƒ")
        XCTAssert(b.dataValue == "abc🌊déƒ".data(using: .utf8))
        XCTAssert((try Blackbird.Value.fromAny("abc🌊déƒ".data(using: .utf8)!)) == b)

        let date = Date()
        XCTAssert((try Blackbird.Value.fromAny(date)) == .double(date.timeIntervalSince1970))
        
        let url = URL(string: "https://www.marco.org/")!
        XCTAssert((try Blackbird.Value.fromAny(url)) == .text(url.absoluteString))
    }

    func testOpenDB() async throws {
        let db = try Blackbird.Database(path: sqliteFilename, options: [.debugPrintEveryQuery, .debugPrintEveryReportedChange])
        try await TestModel.resolveSchema(in: db)
        try await SchemaChangeAddColumnsInitial.resolveSchema(in: db)
        try await SchemaChangeRebuildTableInitial.resolveSchema(in: db)
        await db.close()
    }
    
    func testWhereIdIN() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        let count = min(TestData.URLs.count, TestData.titles.count, TestData.descriptions.count)
        
        try await db.transaction { core in
            for i in 0..<count {
                let m = TestModelWithDescription(id: i, url: TestData.URLs[i], title: TestData.titles[i], description: TestData.descriptions[i])
                try m.writeIsolated(to: db, core: core)
            }
        }
        db.debugPrintCachePerformanceMetrics()

        var giantIDBatch = Array(0...(db.maxQueryVariableCount * 2))
        giantIDBatch.shuffle()
        let all = try await TestModelWithDescription.read(from: db, primaryKeys: giantIDBatch)
        XCTAssert(all.count == count)
        db.debugPrintCachePerformanceMetrics()
        
        var idSet = Set<Int>()
        for m in all { idSet.insert(m.id) }
        for i in 0..<count { XCTAssert(idSet.contains(i)) }
        
        let pkOrder = [ 999, 1, 78, 128, 63, 100000, 571 ]
        let sorted = try await TestModelWithDescription.read(from: db, primaryKeys: pkOrder, preserveOrder: true)
        XCTAssert(sorted[0].id == 999);
        XCTAssert(sorted[1].id == 1);
        XCTAssert(sorted[2].id == 78);
        XCTAssert(sorted[3].id == 128);
        XCTAssert(sorted[4].id == 63);
        XCTAssert(sorted[5].id == 571);
        
        db.debugPrintCachePerformanceMetrics()
    }
    
    func testQueries() async throws {
        let allFilenames = Blackbird.Database.allFilePaths(for: sqliteFilename)
        print("SQLite filenames:\n\(allFilenames.joined(separator: "\n"))")
    
        let db = try Blackbird.Database(path: sqliteFilename, options: [.debugPrintEveryQuery, .debugPrintQueryParameterValues, .debugPrintEveryReportedChange])
        let count = min(TestData.URLs.count, TestData.titles.count, TestData.descriptions.count)
        
        try await db.transaction { core in
            for i in 0..<count {
                let m = TestModelWithDescription(id: i, url: TestData.URLs[i], title: TestData.titles[i], description: TestData.descriptions[i])
                try m.writeIsolated(to: db, core: core)
            }
        }
        
        for i: Int64 in 1..<10 {
            try await MulticolumnPrimaryKeyTest(userID: i, feedID: i, episodeID: i).write(to: db)
        }
        
        let countReturned = try await TestModelWithDescription.count(in: db)
        XCTAssert(countReturned == 1000)

        let countReturnedMatching = try await TestModelWithDescription.count(in: db, matching: \.$id >= 500)
        XCTAssert(countReturnedMatching == 500)

        let the = try await TestModelWithDescription.read(from: db, sqlWhere: "title LIKE 'the%'")
        XCTAssert(the.count == 231)

        let paramFormat1Results = try await TestModelWithDescription.read(from: db, sqlWhere: "title LIKE ?", "the%")
        XCTAssert(paramFormat1Results.count == 231)

        let paramFormat2Results = try await TestModelWithDescription.read(from: db, sqlWhere: "title LIKE ?", arguments: ["the%"])
        XCTAssert(paramFormat2Results.count == 231)

        let paramFormat3Results = try await TestModelWithDescription.read(from: db, sqlWhere: "title LIKE :title", arguments: [":title" : "the%"])
        XCTAssert(paramFormat3Results.count == 231)

        let paramFormat4Results = try await TestModelWithDescription.read(from: db, sqlWhere: "\(\TestModelWithDescription.$title) LIKE :title", arguments: [":title" : "the%"])
        XCTAssert(paramFormat4Results.count == 231)

        // Structured queries
        let first100 =  try await TestModelWithDescription.read(from: db, orderBy: .ascending(\.$id), limit: 100)
        let matches0a = try await TestModelWithDescription.read(from: db, matching: \.$id == 123)
        let matches0b = try await TestModelWithDescription.read(from: db, matching: \.$id == 123 && \.$title == "Hi" || \.$id > 2)
        let matches0c = try await TestModelWithDescription.read(from: db, matching: \.$url != nil)
        let matches0d = try await TestModelWithDescription.read(from: db, matching: .valueIn(\.$id, [1, 2, 3]))
        let matches0e = try await TestModelWithDescription.read(from: db, matching: .like(\.$title, "the%"))
        let matches0f = try await TestModelWithDescription.read(from: db, matching: .like(\.$title, "% % % % %"))
        let matches0g = try await TestModelWithDescription.read(from: db, matching: !.valueIn(\.$id, [1, 2, 3]))

        XCTAssert(first100.count == 100)
        XCTAssert(first100.first!.id == 0)
        XCTAssert(first100.last!.id == 99)
        XCTAssert(matches0a.count == 1)
        XCTAssert(matches0a.first!.id == 123)
        XCTAssert(matches0b.count == 997)
        XCTAssert(matches0c.count == 1000)
        XCTAssert(matches0d.count == 3)
        XCTAssert(matches0e.count == 231)
        XCTAssert(matches0f.count == 235)
        XCTAssert(matches0g.count == 997)

        try await MulticolumnPrimaryKeyTest.update(in: db, set: [\.$episodeID: 5], forMulticolumnPrimaryKeys: [[1, 1, 1], [2, 2, 2], [3, 1, 1]])
        let multiID1 = try await MulticolumnPrimaryKeyTest.read(from: db, multicolumnPrimaryKey: [1, 1, 5])
        let multiID2 = try await MulticolumnPrimaryKeyTest.read(from: db, multicolumnPrimaryKey: [2, 2, 5])
        let multiID3 = try await MulticolumnPrimaryKeyTest.read(from: db, multicolumnPrimaryKey: [3, 3, 5])
        XCTAssert(multiID1!.episodeID == 5)
        XCTAssert(multiID2!.episodeID == 5)
        XCTAssert(multiID3 == nil)

        try await TestModelWithDescription.update(in: db, set: [\.$title: "(new)"], forPrimaryKeys: [1, 2, 3])
        let id1 = try await TestModelWithDescription.read(from: db, id: 1)
        let id2 = try await TestModelWithDescription.read(from: db, id: 2)
        let id3 = try await TestModelWithDescription.read(from: db, id: 3)
        XCTAssert(id1!.title == "(new)")
        XCTAssert(id2!.title == "(new)")
        XCTAssert(id3!.title == "(new)")

        var id42 = try await TestModelWithDescription.read(from: db, id: 42)
        XCTAssertNotNil(id42)
        XCTAssert(id42!.id == 42)
        
        id42?.url = nil
        try await id42?.write(to: db)
        
        try await id42!.delete(from: db)
        let id42AfterDelete = try await TestModelWithDescription.read(from: db, id: 42)
        XCTAssertNil(id42AfterDelete)
        
        let id43 = try await TestModelWithDescription.read(from: db, matching: \.$id == 43).first
        XCTAssertNotNil(id43)
        XCTAssertNotNil(id43!.id == 43)
        try await TestModelWithDescription.delete(from: db, matching: \.$id == 43)
        let id43AfterDelete = try await TestModelWithDescription.read(from: db, matching: \.$id == 43).first
        XCTAssertNil(id43AfterDelete)
        
        let matches1 = try await TestModelWithDescription.read(from: db, orderBy: .descending(\.$title), .ascending(\.$id), limit: 1)
        XCTAssert(matches1.first!.title == "the memory palace")

        let matches = try await TestModelWithDescription.read(from: db, matching: \.$title == "Omnibus")
        XCTAssert(matches.count == 1)
        XCTAssert(matches.first!.title == "Omnibus")
        
        let rows = try await TestModelWithDescription.query(in: db, columns: [\.$id, \.$title, \.$url], matching: \.$title == "Omnibus")
        XCTAssert(rows.count == 1)
        XCTAssert(rows.first!.count == 3)

        let omnibusID = rows.first![\.$id]
        let title = rows.first![\.$title]
        let url = rows.first![\.$url]
        XCTAssert(title == "Omnibus")
        XCTAssert(url != nil)

        try await TestModelWithDescription.update(in: db,set: [ \.$url: nil ], matching: \.$id == omnibusID)

        let rowsWithNilURL = try await TestModelWithDescription.query(in: db, columns: [\.$id, \.$url], matching: \.$url == nil)
        let id = rowsWithNilURL.first![\.$id]
        let nilURL = rowsWithNilURL.first![\.$url]
        XCTAssert(id == omnibusID)
        XCTAssert(nilURL == nil)
        
        try await TestModelWithDescription.delete(from: db, matching: \.$url == nil)
        let leftovers1 = try await TestModelWithDescription.read(from: db, matching: \.$url == nil)
        let leftovers2 = try await TestModelWithDescription.read(from: db, matching: \.$id == omnibusID)
        XCTAssert(leftovers1.isEmpty)
        XCTAssert(leftovers2.isEmpty)

        db.debugPrintCachePerformanceMetrics()
    }

    func testColumnTypes() async throws {
        let db = try Blackbird.Database(path: sqliteFilename, options: [.debugPrintEveryQuery, .debugPrintEveryReportedChange, .debugPrintQueryParameterValues])
        try await TypeTest.resolveSchema(in: db)
        
        let now = Date()
        
        let tt = TypeTest(id: Int64.max, typeIntNull: nil, typeIntNotNull: Int64.min, typeTextNull: nil, typeTextNotNull: "textNotNull!", typeDoubleNull: nil, typeDoubleNotNull: Double.pi, typeDataNull: nil, typeDataNotNull: "dataNotNull!".data(using: .utf8)!, typeIntEnum: .two, typeIntEnumNullWithValue: .one, typeStringEnum: .one, typeStringEnumNullWithValue: .two, typeIntNonZeroEnum: .two, typeIntNonZeroEnumNullWithValue: .two, typeStringNonEmptyEnum: .one, typeStringNonEmptyEnumNullWithValue: .two, typeURLNull: nil, typeURLNotNull: URL(string: "https://marco.org/")!, typeDateNull: nil, typeDateNotNull: now)
        try await tt.write(to: db)
        
        let read = try await TypeTest.read(from: db, id: Int64.max)
        XCTAssertNotNil(read)
        XCTAssert(read!.id == Int64.max)
        XCTAssert(read!.typeIntNull == nil)
        XCTAssert(read!.typeIntNotNull == Int64.min)
        XCTAssert(read!.typeTextNull == nil)
        XCTAssert(read!.typeTextNotNull == "textNotNull!")
        XCTAssert(read!.typeDoubleNull == nil)
        XCTAssert(read!.typeDoubleNotNull == Double.pi)
        XCTAssert(read!.typeDataNull == nil)
        XCTAssert(read!.typeDataNotNull == "dataNotNull!".data(using: .utf8)!)
        XCTAssert(read!.typeIntEnum == .two)
        XCTAssert(read!.typeIntEnumNull == nil)
        XCTAssert(read!.typeIntEnumNullWithValue == .one)
        XCTAssert(read!.typeStringEnum == .one)
        XCTAssert(read!.typeStringEnumNull == nil)
        XCTAssert(read!.typeStringEnumNullWithValue == .two)
        XCTAssert(read!.typeIntNonZeroEnum == .two)
        XCTAssert(read!.typeIntNonZeroEnumWithDefault == .one)
        XCTAssert(read!.typeIntNonZeroEnumNull == nil)
        XCTAssert(read!.typeIntNonZeroEnumNullWithValue == .two)
        XCTAssert(read!.typeStringNonEmptyEnum == .one)
        XCTAssert(read!.typeStringNonEmptyEnumWithDefault == .two)
        XCTAssert(read!.typeStringNonEmptyEnumNull == nil)
        XCTAssert(read!.typeStringNonEmptyEnumNullWithValue == .two)
        XCTAssert(read!.typeURLNull == nil)
        XCTAssert(read!.typeURLNotNull == URL(string: "https://marco.org/")!)
        XCTAssert(read!.typeDateNull == nil)
        XCTAssert(read!.typeDateNotNull.timeIntervalSince1970 == now.timeIntervalSince1970)
        
        let results1 = try await TypeTest.read(from: db, sqlWhere: "typeIntEnum = ?", TypeTest.RepresentableIntEnum.one)
        XCTAssert(results1.count == 0)

        let results2 = try await TypeTest.read(from: db, sqlWhere: "typeIntEnum = ?", TypeTest.RepresentableIntEnum.two)
        XCTAssert(results2.count == 1)
        XCTAssert(results2.first!.id == Int64.max)

        let results3 = try await TypeTest.read(from: db, sqlWhere: "typeStringEnum = ?", TypeTest.RepresentableStringEnum.two)
        XCTAssert(results3.count == 0)

        let results4 = try await TypeTest.read(from: db, sqlWhere: "typeStringEnum = ?", TypeTest.RepresentableStringEnum.one)
        XCTAssert(results4.count == 1)
        XCTAssert(results4.first!.id == Int64.max)
    }

    func testJSONSerialization() async throws {
        let db = try Blackbird.Database(path: sqliteFilename, options: [.debugPrintEveryQuery, .debugPrintEveryReportedChange])
        let count = min(TestData.URLs.count, TestData.titles.count, TestData.descriptions.count)
        try await db.transaction { core in
            for i in 0..<count {
                let m = TestModelWithDescription(id: i, url: TestData.URLs[i], title: TestData.titles[i], description: TestData.descriptions[i])
                try m.writeIsolated(to: db, core: core)
            }
        }

        let the = try await TestModelWithDescription.read(from: db, sqlWhere: "title LIKE 'the%'")
        XCTAssert(the.count == 231)
        
        let results = [
            TestModel(id: 1, title: TestData.randomTitle, url: TestData.randomURL, nonColumn: TestData.randomString(length: 4)),
            TestModel(id: 2, title: TestData.randomTitle, url: TestData.randomURL, nonColumn: TestData.randomString(length: 4)),
            TestModel(id: 3, title: TestData.randomTitle, url: TestData.randomURL, nonColumn: TestData.randomString(length: 4)),
            TestModel(id: 4, title: TestData.randomTitle, url: TestData.randomURL, nonColumn: TestData.randomString(length: 4)),
        ]

        let encoder = JSONEncoder()
        let json = try encoder.encode(results)
        print("json: \(String(data: json, encoding: .utf8)!)")
        
        let decoder = JSONDecoder()
        let decoded = try decoder.decode([TestModel].self, from: json)
        XCTAssert(decoded == results)
        
        for i in 0..<3 {
            let m1 = results[i]
            let m2 = decoded[i]
            XCTAssert(m1.id == m2.id)
            XCTAssert(m1.title == m2.title)
            XCTAssert(m1.url == m2.url)
            XCTAssert(m1.nonColumn == m2.nonColumn)
        }
    }


    func testHeavyWorkload() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)

        // big block of writes to populate the DB
        try await db.transaction { core in
            for i in 0..<1000 {
                let t = TestModel(id: Int64(i), title: TestData.randomTitle, url: TestData.randomURL, nonColumn: TestData.randomDescription)
                try t.writeIsolated(to: db, core: core)
            }
        }

        // random reads/writes interleaved
        for _ in 0..<500 {
            // Attempt 10 random reads
            for _ in 0..<10 {
                _ = try await TestModel.read(from: db, id: Int64.random(in: 0..<1000))
            }
            
            // Random UPDATE
            if var r = try await TestModel.read(from: db, id: Int64.random(in: 0..<1000)) {
                r.title = TestData.randomTitle
                try await r.write(to: db)
            }
            
            // Random INSERT
            let t = TestModel(id: TestData.randomInt64(), title: TestData.randomTitle, url: TestData.randomURL, nonColumn: TestData.randomDescription)
            try await t.write(to: db)
        }

        db.debugPrintCachePerformanceMetrics()

        await db.close()
    }

    func testMemoryDB() async throws {
        sqliteFilename = ":memory:"
        try await testHeavyWorkload()
    }
    
    func testMultiStatements() async throws {
        let db = try Blackbird.Database(path: sqliteFilename, options: [.debugPrintEveryQuery, .debugPrintEveryReportedChange])
        try await TestModel.resolveSchema(in: db)
        try await db.execute("PRAGMA user_version = 234; UPDATE TestModel SET url = NULL")
        let userVersion = try await db.query("PRAGMA user_version").first?["user_version"]
        XCTAssert(userVersion != nil)
        XCTAssert(userVersion!.intValue == 234)
    }

    func testTransactionRollback() async throws {
        let db = try Blackbird.Database(path: sqliteFilename, options: [.debugPrintEveryQuery, .debugPrintEveryReportedChange])
        
        let id = TestData.randomInt64()
        let originalTitle = TestData.randomTitle
        let t = TestModel(id: id, title: originalTitle, url: TestData.randomURL, nonColumn: TestData.randomString(length: 32))
        try await t.write(to: db)

        try await db.transaction { core in
            
        }

        let retVal0 = try await db.transaction { core in
            return "test0"
        }
        XCTAssert(retVal0 == "test0")

        let retVal1Void = try await db.cancellableTransaction { core in
            throw Blackbird.Error.cancelTransaction
        }
        switch retVal1Void {
            case .rolledBack: XCTAssert(true)
            case .committed(_): XCTAssert(false)
        }

        let cancelTransaction = true
        let retVal1 = try await db.cancellableTransaction { core in
            var t = t
            t.title = "new title"
            try t.writeIsolated(to: db, core: core)
            
            let title = try core.query("SELECT title FROM TestModel WHERE id = ?", id).first!["title"]!.stringValue
            XCTAssert(title == "new title")
            
            if (cancelTransaction) {
                throw Blackbird.Error.cancelTransaction
            } else {
                return "Test"
            }
        }
        
        switch retVal1 {
            case .rolledBack: XCTAssert(true)
            case .committed(_): XCTAssert(false)
        }

        let title = try await db.query("SELECT title FROM TestModel WHERE id = ?", id).first!["title"]!.stringValue
        XCTAssert(title == originalTitle)

        let retVal2 = try await db.cancellableTransaction { core in
            var t = t
            t.title = "new title"
            try t.writeIsolated(to: db, core: core)
            
            let title = try core.query("SELECT title FROM TestModel WHERE id = ?", id).first!["title"]!.stringValue
            XCTAssert(title == "new title")
            
            return "Test"
        }
        
        switch retVal2 {
            case .rolledBack: XCTAssert(false)
            case .committed(_): XCTAssert(true)
        }

        let title2 = try await db.query("SELECT title FROM TestModel WHERE id = ?", id).first!["title"]!.stringValue
        XCTAssert(title2 == "new title")
    }

    func testConcurrentAccessToSameDBFile() async throws {
        let mem1 = try Blackbird.Database.inMemoryDatabase(options: [.debugPrintEveryQuery, .debugPrintEveryReportedChange])
        XCTAssertNoThrow(try _ = Blackbird.Database.inMemoryDatabase())
        try await mem1.execute("PRAGMA user_version = 1") // so mem1 doesn't get deallocated until after this

        let db1 = try Blackbird.Database(path: sqliteFilename)
        XCTAssertThrowsError(try _ = Blackbird.Database(path: sqliteFilename))
        await db1.close()
        XCTAssertNoThrow(try Blackbird.Database(path: sqliteFilename)) // should be OK to reuse a path after .close()
    
        await AssertThrowsErrorAsync(try await db1.execute("PRAGMA user_version = 1")) // so db1 doesn't get deallocated until after this and we test throwing errors for accessing a closed DB
    }
    
    func testCodingKeys() async throws {
        let db = try Blackbird.Database(path: sqliteFilename, options: [.debugPrintEveryQuery, .debugPrintEveryReportedChange])
        
        let id = TestData.randomInt64()
        let title = TestData.randomTitle
        let desc = TestData.randomDescription
        
        let t = TestCodingKeys(id: id, title: title, description: desc)
        try await t.write(to: db)
        
        let readBack = try await TestCodingKeys.read(from: db, id: id)
        XCTAssertNotNil(readBack)
        XCTAssert(readBack!.id == id)
        XCTAssert(readBack!.title == title)
        XCTAssert(readBack!.description == desc)
        
        let jsonEncoder = JSONEncoder()
        let data = try jsonEncoder.encode(readBack)
        let decoder = JSONDecoder()
        let decoded = try decoder.decode(TestCodingKeys.self, from: data)
        XCTAssert(decoded.id == id)
        XCTAssert(decoded.title == title)
        XCTAssert(decoded.description == desc)

        let custom = try decoder.decode(TestCustomDecoder.self, from: """
            {"idStr":"123","nameStr":"abc","thumbStr":"https://google.com/"}
        """.data(using: .utf8)!)
        XCTAssert(custom.id == 123)
        XCTAssert(custom.name == "abc")
        XCTAssert(custom.thumbnail == URL(string: "https://google.com/")!)
        
        try await custom.write(to: db)
    }

    func testSchemaChangeAddPrimaryKeyColumn() async throws {
        let userID = TestData.randomInt64()
        let feedID = TestData.randomInt64()
        let episodeID = TestData.randomInt64()

        var db = try Blackbird.Database(path: sqliteFilename, options: [.debugPrintEveryQuery, .debugPrintEveryReportedChange])
        try await SchemaChangeAddPrimaryKeyColumnInitial(userID: userID, feedID: feedID, subscribed: true).write(to: db)
        await db.close()
    
        db = try Blackbird.Database(path: sqliteFilename, options: [.debugPrintEveryQuery, .debugPrintEveryReportedChange])
        let newInstance = SchemaChangeAddPrimaryKeyColumnChanged(userID: userID, feedID: feedID, episodeID: episodeID, subscribed: false)
        try await newInstance.write(to: db)
    
        let firstInstance = try await SchemaChangeAddPrimaryKeyColumnChanged.read(from: db, multicolumnPrimaryKey: [userID, feedID, 0])
        let secondInstance = try await SchemaChangeAddPrimaryKeyColumnChanged.read(from: db, multicolumnPrimaryKey: [userID, feedID, episodeID])
        let thirdInstance = try await SchemaChangeAddPrimaryKeyColumnChanged.read(from: db, multicolumnPrimaryKey: ["userID" : userID, "feedID" : feedID, "episodeID": episodeID])

        XCTAssertNotNil(firstInstance)
        XCTAssertNotNil(secondInstance)
        XCTAssertNotNil(thirdInstance)
        XCTAssert(firstInstance!.episodeID == 0)
        XCTAssert(secondInstance!.episodeID == episodeID)
        XCTAssert(thirdInstance!.episodeID == episodeID)
        XCTAssert(firstInstance!.subscribed == true)
        XCTAssert(secondInstance!.subscribed == false)
        XCTAssert(thirdInstance!.subscribed == false)
    }

    func testSchemaChangeAddColumns() async throws {
        let id = TestData.randomInt64()
        let title = TestData.randomTitle

        var db = try Blackbird.Database(path: sqliteFilename, options: [.debugPrintEveryQuery, .debugPrintEveryReportedChange])
        try await SchemaChangeAddColumnsInitial(id: id, title: title).write(to: db)
        await db.close()
    
        db = try Blackbird.Database(path: sqliteFilename, options: [.debugPrintEveryQuery, .debugPrintEveryReportedChange])
        let newInstance = SchemaChangeAddColumnsChanged(id: TestData.randomInt64(not: id), title: TestData.randomTitle, description: "Custom", url: TestData.randomURL, art: TestData.randomData(length: 2048))
        try await newInstance.write(to: db)
    
        let modifiedInstance = try await SchemaChangeAddColumnsChanged.read(from: db, id: id)
        XCTAssertNotNil(modifiedInstance)
        XCTAssert(modifiedInstance!.title == title)

        let readNewInstance = try await SchemaChangeAddColumnsChanged.read(from: db, id: newInstance.id)
        XCTAssertNotNil(readNewInstance)
        XCTAssert(readNewInstance!.description == "Custom")
    }

    func testSchemaChangeDropColumns() async throws {
        let id = TestData.randomInt64()
        let title = TestData.randomTitle

        var db = try Blackbird.Database(path: sqliteFilename, options: [.debugPrintEveryQuery, .debugPrintEveryReportedChange])
        try await SchemaChangeAddColumnsChanged(id: id, title: title, description: "Custom", url: TestData.randomURL, art: TestData.randomData(length: 2048)).write(to: db)
        await db.close()
    
        db = try Blackbird.Database(path: sqliteFilename, options: [.debugPrintEveryQuery, .debugPrintEveryReportedChange])
        let newInstance = SchemaChangeAddColumnsInitial(id: TestData.randomInt64(not: id), title: TestData.randomTitle)
        try await newInstance.write(to: db)
    
        let modifiedInstance = try await SchemaChangeAddColumnsInitial.read(from: db, id: id)
        XCTAssertNotNil(modifiedInstance)
        XCTAssert(modifiedInstance!.title == title)
    }

    func testSchemaChangeAddIndex() async throws {
        let id = TestData.randomInt64()
        let title = TestData.randomTitle

        var db = try Blackbird.Database(path: sqliteFilename, options: [.debugPrintEveryQuery, .debugPrintEveryReportedChange])
        try await SchemaChangeAddIndexInitial(id: id, title: title).write(to: db)
        await db.close()
    
        db = try Blackbird.Database(path: sqliteFilename, options: [.debugPrintEveryQuery, .debugPrintEveryReportedChange])
        let newInstance = SchemaChangeAddIndexChanged(id: TestData.randomInt64(not: id), title: TestData.randomTitle)
        try await newInstance.write(to: db)
    
        let modifiedInstance = try await SchemaChangeAddIndexChanged.read(from: db, id: id)
        XCTAssertNotNil(modifiedInstance)
        XCTAssert(modifiedInstance!.title == title)
    }

    func testSchemaChangeDropIndex() async throws {
        let id = TestData.randomInt64()
        let title = TestData.randomTitle

        var db = try Blackbird.Database(path: sqliteFilename, options: [.debugPrintEveryQuery, .debugPrintEveryReportedChange])
        try await SchemaChangeAddIndexChanged(id: id, title: title).write(to: db)
        await db.close()
    
        db = try Blackbird.Database(path: sqliteFilename, options: [.debugPrintEveryQuery, .debugPrintEveryReportedChange])
        let newInstance = SchemaChangeAddIndexInitial(id: TestData.randomInt64(not: id), title: TestData.randomTitle)
        try await newInstance.write(to: db)
    
        let modifiedInstance = try await SchemaChangeAddIndexInitial.read(from: db, id: id)
        XCTAssertNotNil(modifiedInstance)
        XCTAssert(modifiedInstance!.title == title)
    }

    func testSchemaChangeRebuildTable() async throws {
        let id = TestData.randomInt64()
        let title = TestData.randomTitle

        var db = try Blackbird.Database(path: sqliteFilename, options: [.debugPrintEveryQuery, .debugPrintEveryReportedChange])
        try await SchemaChangeRebuildTableInitial(id: id, title: title, flags: 15).write(to: db)
        await db.close()
    
        db = try Blackbird.Database(path: sqliteFilename, options: [.debugPrintEveryQuery, .debugPrintEveryReportedChange])
        let newInstance = SchemaChangeRebuildTableChanged(id: TestData.randomInt64(not: id), title: TestData.randomTitle, flags: "{1,0}", description: TestData.randomDescription)
        try await newInstance.write(to: db)
    
        let modifiedInstance = try await SchemaChangeRebuildTableChanged.read(from: db, id: id)
        XCTAssertNotNil(modifiedInstance)
        XCTAssert(modifiedInstance!.title == title)
        XCTAssert(modifiedInstance!.description == "")
        XCTAssert(modifiedInstance!.flags == "15")
    }
    
    func testColumnChanges() async throws {
        let db = try Blackbird.Database(path: sqliteFilename, options: [.debugPrintEveryQuery, .debugPrintEveryReportedChange])
        let db2 = try Blackbird.Database.inMemoryDatabase()
        
        var t = TestModel(id: TestData.randomInt64(), title: "Original Title", url: TestData.randomURL)
        XCTAssert(t.$id.hasChanged(in: db))
        XCTAssert(t.$title.hasChanged(in: db))
        XCTAssert(t.$url.hasChanged(in: db))
        XCTAssert(t.changedColumns(in: db) == Blackbird.ColumnNames(["id", "title", "url"]))
        XCTAssert(t.$id.hasChanged(in: db2))
        XCTAssert(t.$title.hasChanged(in: db2))
        XCTAssert(t.$url.hasChanged(in: db2))
        XCTAssert(t.changedColumns(in: db2) == Blackbird.ColumnNames(["id", "title", "url"]))

        try await t.write(to: db)

        XCTAssert(!t.$id.hasChanged(in: db))
        XCTAssert(!t.$title.hasChanged(in: db))
        XCTAssert(!t.$url.hasChanged(in: db))
        XCTAssert(t.changedColumns(in: db).isEmpty)
        XCTAssert(t.$id.hasChanged(in: db2))
        XCTAssert(t.$title.hasChanged(in: db2))
        XCTAssert(t.$url.hasChanged(in: db2))
        XCTAssert(t.changedColumns(in: db2) == Blackbird.ColumnNames(["id", "title", "url"]))
        
        t.title = "Updated Title"

        XCTAssert(!t.$id.hasChanged(in: db))
        XCTAssert(t.$title.hasChanged(in: db))
        XCTAssert(!t.$url.hasChanged(in: db))
        XCTAssert(t.changedColumns(in: db) == Blackbird.ColumnNames(["title"]))

        try await t.write(to: db)

        XCTAssert(!t.$id.hasChanged(in: db))
        XCTAssert(!t.$title.hasChanged(in: db))
        XCTAssert(!t.$url.hasChanged(in: db))
        XCTAssert(t.changedColumns(in: db).isEmpty)
        
        var t2 = try await TestModel.read(from: db, id: t.id)!
        XCTAssert(!t2.$id.hasChanged(in: db))
        XCTAssert(!t2.$title.hasChanged(in: db))
        XCTAssert(!t2.$url.hasChanged(in: db))
        XCTAssert(t2.changedColumns(in: db).isEmpty)
        
        t2.title = "Third Title"
        XCTAssert(!t2.$id.hasChanged(in: db))
        XCTAssert(t2.$title.hasChanged(in: db))
        XCTAssert(!t2.$url.hasChanged(in: db))
        XCTAssert(t2.changedColumns(in: db) == Blackbird.ColumnNames(["title"]))
        
        try await t2.write(to: db)

        XCTAssert(!t.$id.hasChanged(in: db))
        XCTAssert(!t.$title.hasChanged(in: db))
        XCTAssert(!t.$url.hasChanged(in: db))
        XCTAssert(t.changedColumns(in: db).isEmpty)
    }
    
    var _testChangeNotificationsExpectedChangedTable: String? = nil
    var _testChangeNotificationsExpectedChangedKeys: Blackbird.PrimaryKeyValues? = nil
    var _testChangeNotificationsExpectedChangedColumnNames: Blackbird.ColumnNames? = nil
    var _testChangeNotificationsListeners: [AnyCancellable] = []
    var _testChangeNotificationsCallCount = 0
    func testChangeNotifications() async throws {
        let db = try Blackbird.Database(path: sqliteFilename, options: [.debugPrintEveryQuery, .debugPrintEveryReportedChange, .debugPrintQueryParameterValues])
        
        try await TestModel.resolveSchema(in: db)
        try await TestModelWithDescription.resolveSchema(in: db)
        
        _testChangeNotificationsListeners.append(TestModel.changePublisher(in: db).sink { change in
            if let expectedTable = self._testChangeNotificationsExpectedChangedTable {
                XCTAssert(expectedTable == change.type.tableName, "Change listener called for incorrect table")
            }
            self._testChangeNotificationsCallCount += 1
        })

        _testChangeNotificationsListeners.append(TestModelWithDescription.changePublisher(in: db).sink { change in
            if change.primaryKeys == nil {
                XCTAssertNil(self._testChangeNotificationsExpectedChangedKeys)
            } else {
                XCTAssertEqual(self._testChangeNotificationsExpectedChangedKeys, change.primaryKeys)
            }
            
            if change.columnNames == nil {
                XCTAssertNil(self._testChangeNotificationsExpectedChangedColumnNames)
            } else {
                XCTAssertEqual(self._testChangeNotificationsExpectedChangedColumnNames, change.columnNames)
            }

            self._testChangeNotificationsCallCount += 1
        })
        
        var expectedChangeNotificationsCallCount = 0
        
        _testChangeNotificationsExpectedChangedTable = "TestModelWithDescription"
        
        // Batched change notifications
        let count = min(TestData.URLs.count, TestData.titles.count, TestData.descriptions.count)
        try await db.transaction { core in
            var expectedBatchedKeys = Blackbird.PrimaryKeyValues()
            for i in 0..<count {
                expectedBatchedKeys.insert([.integer(Int64(i))])
                let m = TestModelWithDescription(id: i, url: TestData.URLs[i], title: TestData.titles[i], description: TestData.descriptions[i])
                try m.writeIsolated(to: db, core: core)
            }
            self._testChangeNotificationsExpectedChangedKeys = expectedBatchedKeys
            self._testChangeNotificationsExpectedChangedColumnNames = Blackbird.ColumnNames(["id", "url", "title", "description"])
        }
        expectedChangeNotificationsCallCount += 1
        XCTAssert(_testChangeNotificationsCallCount == expectedChangeNotificationsCallCount)
        
        // Individual change notifications
        var m = try await TestModelWithDescription.read(from: db, id: 64)!
        m.title = "Edited title!"
        _testChangeNotificationsExpectedChangedKeys = Blackbird.PrimaryKeyValues([[ .integer(64) ]])
        _testChangeNotificationsExpectedChangedColumnNames = Blackbird.ColumnNames(["title"])
        try await m.write(to: db)
        expectedChangeNotificationsCallCount += 1
        XCTAssert(_testChangeNotificationsCallCount == expectedChangeNotificationsCallCount)

        // Unspecified/whole-table change notifications, with structured column info
        _testChangeNotificationsExpectedChangedKeys = Blackbird.PrimaryKeyValues(Array(0..<count).map { [try! Blackbird.Value.fromAny($0)] })
        _testChangeNotificationsExpectedChangedColumnNames = Blackbird.ColumnNames(["url"])
        try await TestModelWithDescription.update(in: db, set: [ \.$url : nil ], matching: .all)
        expectedChangeNotificationsCallCount += 1
        XCTAssert(_testChangeNotificationsCallCount == expectedChangeNotificationsCallCount)

        // Unspecified/whole-table delete notifications, with structured column info
        _testChangeNotificationsExpectedChangedKeys = Blackbird.PrimaryKeyValues(Array(0..<5).map { [try! Blackbird.Value.fromAny($0)] })
        _testChangeNotificationsExpectedChangedColumnNames = nil
        try await TestModelWithDescription.delete(from: db, matching: \.$id < 5)
        expectedChangeNotificationsCallCount += 1
        XCTAssert(_testChangeNotificationsCallCount == expectedChangeNotificationsCallCount)

        // Unspecified/whole-table change notifications, with structured column info and primary keys
        _testChangeNotificationsExpectedChangedKeys = [[7], [8], [9]]
        _testChangeNotificationsExpectedChangedColumnNames = Blackbird.ColumnNames(["url"])
        try await TestModelWithDescription.update(in: db, set: [ \.$url : nil ], forPrimaryKeys: [7, 8, 9])
        expectedChangeNotificationsCallCount += 1
        XCTAssert(_testChangeNotificationsCallCount == expectedChangeNotificationsCallCount)

        // Unspecified/whole-table change notifications, structured, but affecting 0 rows -- no change notification expected
        _testChangeNotificationsExpectedChangedKeys = nil
        _testChangeNotificationsExpectedChangedColumnNames = nil
        try await TestModelWithDescription.update(in: db, set: [ \.$url : nil ], matching: .all)
        XCTAssert(_testChangeNotificationsCallCount == expectedChangeNotificationsCallCount)

        // Unspecified/whole-table change notifications
        _testChangeNotificationsExpectedChangedKeys = nil
        _testChangeNotificationsExpectedChangedColumnNames = nil
        try await TestModelWithDescription.query(in: db, "UPDATE $T SET url = NULL")
        expectedChangeNotificationsCallCount += 1
        XCTAssert(_testChangeNotificationsCallCount == expectedChangeNotificationsCallCount)

        // Column-name merging
        _testChangeNotificationsExpectedChangedKeys = Blackbird.PrimaryKeyValues([[ .integer(31) ], [ .integer(32) ]])
        _testChangeNotificationsExpectedChangedColumnNames = Blackbird.ColumnNames(["title", "description"])
        try await db.transaction { core in
            var t1 = try TestModelWithDescription.readIsolated(from: db, core: core, id: 31)!
            t1.title = "Edited title!"
            var t2 = try TestModelWithDescription.readIsolated(from: db, core: core, id: 32)!
            t2.description = "Edited description!"
            
            try t1.writeIsolated(to: db, core: core)
            try t2.writeIsolated(to: db, core: core)
        }
        expectedChangeNotificationsCallCount += 1
        XCTAssert(_testChangeNotificationsCallCount == expectedChangeNotificationsCallCount)

        // Merging with insertions
        _testChangeNotificationsExpectedChangedKeys = Blackbird.PrimaryKeyValues([[ .integer(40) ], [ .integer(Int64(count) + 1) ]])
        _testChangeNotificationsExpectedChangedColumnNames = Blackbird.ColumnNames(["id", "title", "description", "url"])
        try await db.transaction { core in
            var t1 = try TestModelWithDescription.readIsolated(from: db, core: core, id: 40)!
            t1.title = "Edited title!"
            try t1.writeIsolated(to: db, core: core)
            
            let t2 = TestModelWithDescription(id: count + 1, title: "New entry", description: "New description")
            try t2.writeIsolated(to: db, core: core)
        }
        expectedChangeNotificationsCallCount += 1
        XCTAssert(_testChangeNotificationsCallCount == expectedChangeNotificationsCallCount)

        // Merging with deletions
        _testChangeNotificationsExpectedChangedKeys = Blackbird.PrimaryKeyValues([[ .integer(50) ], [ .integer(51) ]])
        _testChangeNotificationsExpectedChangedColumnNames = Blackbird.ColumnNames(["id", "title", "description", "url"])
        try await db.transaction { core in
            var t1 = try TestModelWithDescription.readIsolated(from: db, core: core, id: 50)!
            t1.title = "Edited title!"
            try t1.writeIsolated(to: db, core: core)

            let t2 = try TestModelWithDescription.readIsolated(from: db, core: core, id: 51)!
            try t2.deleteIsolated(from: db, core: core)
        }
        expectedChangeNotificationsCallCount += 1
        XCTAssert(_testChangeNotificationsCallCount == expectedChangeNotificationsCallCount)

        // Merging with table-wide updates
        _testChangeNotificationsExpectedChangedKeys = nil
        _testChangeNotificationsExpectedChangedColumnNames = nil
        try await db.transaction { core in
            var t1 = try TestModelWithDescription.readIsolated(from: db, core: core, id: 60)!
            t1.title = "Edited title!"
            try t1.writeIsolated(to: db, core: core)

            try TestModelWithDescription.queryIsolated(in: db, core: core, "UPDATE $T SET description = ? WHERE id = 61", "Test description")
        }
        expectedChangeNotificationsCallCount += 1
        XCTAssert(_testChangeNotificationsCallCount == expectedChangeNotificationsCallCount)
        

        // ------- Should be the last test in this func since it deletes the entire table -------
        // The SQLite truncate optimization: https://www.sqlite.org/lang_delete.html#the_truncate_optimization
        _testChangeNotificationsExpectedChangedTable = nil
        _testChangeNotificationsExpectedChangedKeys = nil
        _testChangeNotificationsExpectedChangedColumnNames = nil
        try await TestModelWithDescription.query(in: db, "DELETE FROM $T")
        expectedChangeNotificationsCallCount += 2 // will trigger a full-database change notification, so it'll report 2 table changes: TestModel and TestModelWithDescription
        XCTAssert(_testChangeNotificationsCallCount == expectedChangeNotificationsCallCount)
    }

    func testKeyPathInterpolation() async throws {
        let str = "SELECT \(\TestModel.$title)"
        XCTAssert(str == "SELECT title")
    }
    
    func testOptionalColumn() async throws {
        let db = try Blackbird.Database.inMemoryDatabase(options: [.debugPrintEveryQuery, .debugPrintQueryParameterValues])
        
        let testDate = Date()
        let testURL = URL(string: "https://github.com/marcoarment/Blackbird")!
        let testData = "Hi".data(using: .utf8)
        try await TestModelWithOptionalColumns(id: 1, date: Date(), name: "a").write(to: db)
        try await TestModelWithOptionalColumns(id: 2, date: Date(), name: "b", value: "2").write(to: db)
        try await TestModelWithOptionalColumns(id: 3, date: Date(), name: "c", value: "3", otherValue: 30).write(to: db)
        try await TestModelWithOptionalColumns(id: 4, date: Date(), name: "d", value: "4", optionalDate: testDate).write(to: db)
        try await TestModelWithOptionalColumns(id: 5, date: Date(), name: "e", value: "5", optionalURL: testURL).write(to: db)
        try await TestModelWithOptionalColumns(id: 6, date: Date(), name: "f", value: "6", optionalData: testData).write(to: db)
        
        let t1 = try await TestModelWithOptionalColumns.read(from: db, id: 1)!
        let t2 = try await TestModelWithOptionalColumns.read(from: db, id: 2)!
        let t3 = try await TestModelWithOptionalColumns.read(from: db, id: 3)!
        let t4 = try await TestModelWithOptionalColumns.read(from: db, id: 4)!
        let t5 = try await TestModelWithOptionalColumns.read(from: db, id: 5)!
        let t6 = try await TestModelWithOptionalColumns.read(from: db, id: 6)!

        XCTAssert(t1.name == "a")
        XCTAssert(t2.name == "b")
        XCTAssert(t3.name == "c")
        XCTAssert(t4.name == "d")
        XCTAssert(t5.name == "e")
        XCTAssert(t6.name == "f")

        XCTAssert(t1.value == nil)
        XCTAssert(t2.value == "2")
        XCTAssert(t3.value == "3")
        XCTAssert(t4.value == "4")
        XCTAssert(t5.value == "5")
        XCTAssert(t6.value == "6")

        XCTAssert(t1.otherValue == nil)
        XCTAssert(t2.otherValue == nil)
        XCTAssert(t3.otherValue == 30)
        XCTAssert(t4.otherValue == nil)
        XCTAssert(t5.otherValue == nil)
        XCTAssert(t6.otherValue == nil)

        XCTAssert(t1.optionalDate == nil)
        XCTAssert(t2.optionalDate == nil)
        XCTAssert(t3.optionalDate == nil)
        XCTAssert(abs(t4.optionalDate!.timeIntervalSince(testDate)) < 0.001)
        XCTAssert(t5.optionalDate == nil)
        XCTAssert(t6.optionalDate == nil)

        XCTAssert(t1.optionalURL == nil)
        XCTAssert(t2.optionalURL == nil)
        XCTAssert(t3.optionalURL == nil)
        XCTAssert(t4.optionalURL == nil)
        XCTAssert(t5.optionalURL == testURL)
        XCTAssert(t6.optionalURL == nil)

        XCTAssert(t1.optionalData == nil)
        XCTAssert(t2.optionalData == nil)
        XCTAssert(t3.optionalData == nil)
        XCTAssert(t4.optionalData == nil)
        XCTAssert(t5.optionalData == nil)
        XCTAssert(t6.optionalData == testData)
        
        let random = try await TestModelWithOptionalColumns.read(from: db, matching: .literal("id % 5 = ?", 3))
        XCTAssert(random.count == 1)
        XCTAssert(random.first!.id == 3)

        try await TestModelWithOptionalColumns.delete(from: db, matching: .all)
        let results = try await TestModelWithOptionalColumns.read(from: db, matching: .all)
        XCTAssert(results.count == 0)
    }

    func testUniqueIndex() async throws {
        let db = try Blackbird.Database.inMemoryDatabase(options: [.debugPrintEveryQuery, .debugPrintQueryParameterValues])

        let testDate = Date()
        try await TestModelWithUniqueIndex(id: 1, a: "a1", b: 100, c: testDate).write(to: db)
        try await TestModelWithUniqueIndex(id: 2, a: "a2", b: 200, c: testDate).write(to: db)
        
        var caughtExpectedError = false
        do {
            try await TestModelWithUniqueIndex(id: 3, a: "a2", b: 200, c: testDate).write(to: db)
        } catch Blackbird.Database.Error.uniqueConstraintFailed {
            caughtExpectedError = true
        }
        XCTAssert(caughtExpectedError)

        let allBefore = try await TestModelWithUniqueIndex.read(from: db, sqlWhere: "1 ORDER BY id")
        XCTAssert(allBefore.count == 2)

        XCTAssert(allBefore[0].id == 1)
        XCTAssert(allBefore[0].a == "a1")
        XCTAssert(allBefore[0].b == 100)

        XCTAssert(allBefore[1].id == 2)
        XCTAssert(allBefore[1].a == "a2")
        XCTAssert(allBefore[1].b == 200)

        try await TestModelWithUniqueIndex(id: 3, a: "a2", b: 201, c: testDate).write(to: db)
        
        let all = try await TestModelWithUniqueIndex.read(from: db, sqlWhere: "1 ORDER BY id")
        XCTAssert(all.count == 3)

        XCTAssert(all[0].id == 1)
        XCTAssert(all[0].a == "a1")
        XCTAssert(all[0].b == 100)

        XCTAssert(all[1].id == 2)
        XCTAssert(all[1].a == "a2")
        XCTAssert(all[1].b == 200)

        XCTAssert(all[2].id == 3)
        XCTAssert(all[2].a == "a2")
        XCTAssert(all[2].b == 201)

    }
    
    func testCache() async throws {
        TestModel.cacheLimit = 10000

        let db = try Blackbird.Database(path: sqliteFilename)

        // big block of writes to populate the DB
        let lastURL = try await db.transaction { core in
            var lastURL: URL?
            for i in 0..<1000 {
                let t = TestModel(id: Int64(i), title: TestData.randomTitle, url: TestData.randomURL, nonColumn: TestData.randomDescription)
                try t.writeIsolated(to: db, core: core)
                lastURL = t.url
            }
            return lastURL!
        }
        
        db.resetCachePerformanceMetrics(tableName: TestModel.tableName)
        var t = try await TestModel.read(from: db, id: 1)!
        XCTAssert(db.cachePerformanceMetricsByTableName()[TestModel.tableName]!.misses == 0)
        XCTAssert(db.cachePerformanceMetricsByTableName()[TestModel.tableName]!.hits == 1)
        
        db.resetCachePerformanceMetrics(tableName: TestModel.tableName)
        t.title = "new"
        try await t.write(to: db)
        XCTAssert(db.cachePerformanceMetricsByTableName()[TestModel.tableName]!.writes == 1)
        XCTAssert(db.cachePerformanceMetricsByTableName()[TestModel.tableName]!.rowInvalidations == 1)
        XCTAssert(db.cachePerformanceMetricsByTableName()[TestModel.tableName]!.tableInvalidations == 0)
        
        db.resetCachePerformanceMetrics(tableName: TestModel.tableName)
        t = try await TestModel.read(from: db, id: 1)!
        XCTAssert(t.title == "new")
        XCTAssert(db.cachePerformanceMetricsByTableName()[TestModel.tableName]!.misses == 0)
        XCTAssert(db.cachePerformanceMetricsByTableName()[TestModel.tableName]!.hits == 1)

        db.resetCachePerformanceMetrics(tableName: TestModel.tableName)
        try await db.query("UPDATE TestModel SET title = 'new2' WHERE id = 1")
        t = try await TestModel.read(from: db, id: 1)!
        XCTAssert(t.title == "new2")
        XCTAssert(db.cachePerformanceMetricsByTableName()[TestModel.tableName]!.misses == 1)
        XCTAssert(db.cachePerformanceMetricsByTableName()[TestModel.tableName]!.hits == 0)
        XCTAssert(db.cachePerformanceMetricsByTableName()[TestModel.tableName]!.rowInvalidations == 0)
        XCTAssert(db.cachePerformanceMetricsByTableName()[TestModel.tableName]!.tableInvalidations == 1)

        db.resetCachePerformanceMetrics(tableName: TestModel.tableName)
        try await TestModel.update(in: db, set: [ \.$title : "new2" ], matching: \.$id == 1)
        t = try await TestModel.read(from: db, id: 1)!
        XCTAssert(db.cachePerformanceMetricsByTableName()[TestModel.tableName]!.misses == 0)
        XCTAssert(db.cachePerformanceMetricsByTableName()[TestModel.tableName]!.hits == 1)

        db.resetCachePerformanceMetrics(tableName: TestModel.tableName)
        try await TestModel.update(in: db, set: [ \.$title : "new3" ], matching: \.$id < 10)
        t = try await TestModel.read(from: db, id: 1)!
        XCTAssert(t.title == "new3")
        XCTAssert(db.cachePerformanceMetricsByTableName()[TestModel.tableName]!.misses == 1)
        XCTAssert(db.cachePerformanceMetricsByTableName()[TestModel.tableName]!.hits == 0)
        
        db.resetCachePerformanceMetrics(tableName: TestModel.tableName)
        var titleMatch = try await TestModel.query(in: db, columns: [\.$title], matching: \.$url == lastURL)
        XCTAssert(!titleMatch.isEmpty)
        XCTAssert(db.cachePerformanceMetricsByTableName()[TestModel.tableName]!.misses == 1)
        XCTAssert(db.cachePerformanceMetricsByTableName()[TestModel.tableName]!.hits == 0)
        titleMatch = try await TestModel.query(in: db, columns: [\.$title], matching: \.$url == lastURL)
        XCTAssert(!titleMatch.isEmpty)
        XCTAssert(db.cachePerformanceMetricsByTableName()[TestModel.tableName]!.misses == 1)
        XCTAssert(db.cachePerformanceMetricsByTableName()[TestModel.tableName]!.hits == 1)
        
        t.id = 9998
        try await t.write(to: db)
        XCTAssert(db.cachePerformanceMetricsByTableName()[TestModel.tableName]!.queryInvalidations == 1)
        XCTAssert(db.cachePerformanceMetricsByTableName()[TestModel.tableName]!.rowInvalidations == 0)
        XCTAssert(db.cachePerformanceMetricsByTableName()[TestModel.tableName]!.tableInvalidations == 0)
        
        try await TestModel.update(in: db, set: [\.$id : 9999], matching: \.$id == 1)
        XCTAssert(db.cachePerformanceMetricsByTableName()[TestModel.tableName]!.queryInvalidations == 1)
        XCTAssert(db.cachePerformanceMetricsByTableName()[TestModel.tableName]!.rowInvalidations == 0)
        XCTAssert(db.cachePerformanceMetricsByTableName()[TestModel.tableName]!.tableInvalidations == 0)
    }
    
    func testCacheSpeed() async throws {
        let cacheEnabled = true

        TestModel.cacheLimit = cacheEnabled ? 10000 : 0
        TestModelWithDescription.cacheLimit = TestModel.cacheLimit
        let startTime = Date()
        try await testQueries()
        try await testHeavyWorkload()
        try await testChangeNotifications()
        let duration = abs(startTime.timeIntervalSinceNow)
        print("took \(duration) seconds")
    
//        measure {
//            let exp = expectation(description: "Finished")
//            Task {
//                let startTime = Date()
//                try await testHeavyWorkload()
//                let duration = startTime.timeIntervalSinceNow
//                print("took \(duration) seconds")
//                exp.fulfill()
//            }
//            wait(for: [exp], timeout: 200.0)
//        }
    }

/* Tests duplicate-index detection. Throws fatal error on success.
    func testDuplicateIndex() async throws {
        var db = try Blackbird.Database(path: sqliteFilename, options: [.debugPrintEveryQuery, .debugPrintQueryParameterValues])
        try await DuplicateIndexesModel(id: 1, title: "Hi").write(to: db)
        await db.close()
        
        db = try Blackbird.Database(path: sqliteFilename, options: [.debugPrintEveryQuery, .debugPrintQueryParameterValues])
        try await DuplicateIndexesModel(id: 2, title: "Hi").write(to: db)
    }
*/
}

