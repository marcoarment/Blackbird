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
//  BlackbirdVacuumTests.swift
//  Created by Marco Arment on 7/18/26.
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

final class BlackbirdVacuumTests: XCTestCase {
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

    func testIncrementalVacuum() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await TestModel(id: 1, title: "a", url: URL(string: "https://example.com/1")!).write(to: db)

        // Not yet in incremental mode; conversion not allowed by default
        guard case .needsConversion = try await db.incrementalVacuum(timeLimit: 1) else {
            return XCTFail("expected .needsConversion before conversion is allowed")
        }

        // One-time conversion
        guard case .converted = try await db.incrementalVacuum(timeLimit: 1, allowConversionUpToBytes: .max) else {
            return XCTFail("expected .converted")
        }
        let autoVacuumMode = try await db.query("PRAGMA auto_vacuum").first?["auto_vacuum"]?.int64Value
        XCTAssertEqual(autoVacuumMode, 2)

        // Create free pages: write a bunch of data, then delete it
        for i in 0..<500 {
            try await TestModel(id: Int64(i + 10), title: String(repeating: "x", count: 2048), url: URL(string: "https://example.com/big/\(i)")!).write(to: db)
        }
        try await TestModel.query(in: db, "DELETE FROM $T WHERE id >= 10")

        let freePagesBefore = try await db.query("PRAGMA freelist_count").first?["freelist_count"]?.int64Value ?? 0
        XCTAssertGreaterThan(freePagesBefore, 0)

        // Reclaim within a generous budget; the freelist should empty
        guard case .done(let pagesReclaimed) = try await db.incrementalVacuum(timeLimit: 10) else {
            return XCTFail("expected .done")
        }
        XCTAssertGreaterThan(pagesReclaimed, 0)

        let freePagesAfter = try await db.query("PRAGMA freelist_count").first?["freelist_count"]?.int64Value ?? 0
        XCTAssertEqual(freePagesAfter, 0)

        // Idempotent when there's nothing to reclaim
        guard case .done(let noMorePages) = try await db.incrementalVacuum(timeLimit: 1) else {
            return XCTFail("expected .done on empty freelist")
        }
        XCTAssertEqual(noMorePages, 0)

        await db.close()
    }
}
