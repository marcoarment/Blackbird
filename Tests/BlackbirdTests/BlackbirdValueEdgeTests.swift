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
//  BlackbirdValueEdgeTests.swift
//
//  Edge-case tests for value storage and Blackbird.Value: integer boundaries,
//  special floating-point values, date/string/data/URL round-trips, enum
//  columns with unknown raw values, SQLite-literal parsing with adversarial
//  inputs, fromAny conversions, and the type-coercion accessors.
//

import XCTest
import SQLite3
@testable import Blackbird

// MARK: - Models

struct EdgeValInt64Model: BlackbirdModel {
    static let tableName = "EdgeValInt64Model"
    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var val: Int64
}

struct EdgeValNarrowIntModel: BlackbirdModel {
    static let tableName = "EdgeValNarrowIntModel"
    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var i8: Int8
    @BlackbirdColumn var i16: Int16
    @BlackbirdColumn var i32: Int32
    @BlackbirdColumn var u8: UInt8
    @BlackbirdColumn var u16: UInt16
    @BlackbirdColumn var u32: UInt32
}

struct EdgeValDoubleModel: BlackbirdModel {
    static let tableName = "EdgeValDoubleModel"
    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var d: Double
    @BlackbirdColumn var od: Double?
}

struct EdgeValBoolModel: BlackbirdModel {
    static let tableName = "EdgeValBoolModel"
    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var b: Bool
}

struct EdgeValDateModel: BlackbirdModel {
    static let tableName = "EdgeValDateModel"
    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var date: Date
    @BlackbirdColumn var odate: Date?
}

struct EdgeValStringModel: BlackbirdModel {
    static let tableName = "EdgeValStringModel"
    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var s: String
}

struct EdgeValDataModel: BlackbirdModel {
    static let tableName = "EdgeValDataModel"
    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var data: Data
}

struct EdgeValURLModel: BlackbirdModel {
    static let tableName = "EdgeValURLModel"
    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var url: URL
    @BlackbirdColumn var ourl: URL?
}

struct EdgeValOptionalModel: BlackbirdModel {
    static let tableName = "EdgeValOptionalModel"
    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var s: String?
    @BlackbirdColumn var i: Int?
}

struct EdgeValEnumModel: BlackbirdModel {
    static let tableName = "EdgeValEnumModel"

    enum IntCases: Int, BlackbirdIntegerEnum {
        typealias RawValue = Int
        case zero = 0
        case one = 1
        case negative = -5
    }

    enum StringCases: String, BlackbirdStringEnum {
        typealias RawValue = String
        case empty = ""
        case alpha = "alpha"
        case beta = "beta"
    }

    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var intEnum: IntCases
    @BlackbirdColumn var stringEnum: StringCases
    @BlackbirdColumn var optIntEnum: IntCases?
    @BlackbirdColumn var optStringEnum: StringCases?
}

// MARK: - Tests

final class BlackbirdValueEdgeTests: XCTestCase, @unchecked Sendable {
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

    // MARK: Integer storage

    func testInt64Boundaries() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        let values: [Int64] = [.min, .max, 0, -1]
        for (i, v) in values.enumerated() {
            try await EdgeValInt64Model(id: Int64(i), val: v).write(to: db)
        }
        for (i, v) in values.enumerated() {
            let read = try await EdgeValInt64Model.read(from: db, id: Int64(i))
            XCTAssertEqual(read?.val, v, "Int64 value \(v) did not round-trip")
        }
        await db.close()
    }

    func testNarrowIntegerBoundaries() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)

        let mins = EdgeValNarrowIntModel(id: 1, i8: .min, i16: .min, i32: .min, u8: .min, u16: .min, u32: .min)
        let maxs = EdgeValNarrowIntModel(id: 2, i8: .max, i16: .max, i32: .max, u8: .max, u16: .max, u32: .max)
        try await mins.write(to: db)
        try await maxs.write(to: db)

        let readMins = try await EdgeValNarrowIntModel.read(from: db, id: 1)
        XCTAssertEqual(readMins?.i8, Int8.min)
        XCTAssertEqual(readMins?.i16, Int16.min)
        XCTAssertEqual(readMins?.i32, Int32.min)
        XCTAssertEqual(readMins?.u8, UInt8.min)
        XCTAssertEqual(readMins?.u16, UInt16.min)
        XCTAssertEqual(readMins?.u32, UInt32.min)

        let readMaxs = try await EdgeValNarrowIntModel.read(from: db, id: 2)
        XCTAssertEqual(readMaxs?.i8, Int8.max)
        XCTAssertEqual(readMaxs?.i16, Int16.max)
        XCTAssertEqual(readMaxs?.i32, Int32.max)
        XCTAssertEqual(readMaxs?.u8, UInt8.max)
        XCTAssertEqual(readMaxs?.u16, UInt16.max)
        XCTAssertEqual(readMaxs?.u32, UInt32.max)
        await db.close()
    }

    // Out-of-range stored values must throw (not trap) for every narrow integer type.
    func testNarrowIntegerOutOfRangeThrowsForEachType() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await EdgeValNarrowIntModel(id: 1, i8: 0, i16: 0, i32: 0, u8: 0, u16: 0, u32: 0).write(to: db)

        for (column, badValue) in [("i8", "128"), ("i16", "32768"), ("i32", "2147483648"), ("u8", "-1"), ("u16", "65536"), ("u32", "4294967296")] {
            try await db.query("UPDATE EdgeValNarrowIntModel SET i8=0, i16=0, i32=0, u8=0, u16=0, u32=0")
            try await db.query("UPDATE EdgeValNarrowIntModel SET \(column) = \(badValue)")
            await AssertThrowsErrorAsync(_ = try await EdgeValNarrowIntModel.read(from: db, id: 1))
        }
        await db.close()
    }

    // MARK: Double storage

    func testDoubleSpecialFiniteValues() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        let values: [Double] = [-0.0, .leastNonzeroMagnitude, .greatestFiniteMagnitude, .pi, -.greatestFiniteMagnitude, .leastNormalMagnitude]
        for (i, v) in values.enumerated() {
            try await EdgeValDoubleModel(id: Int64(i), d: v, od: v).write(to: db)
        }
        for (i, v) in values.enumerated() {
            let read = try await EdgeValDoubleModel.read(from: db, id: Int64(i))
            XCTAssertEqual(read?.d, v, "Double \(v) did not round-trip")
            XCTAssertEqual(read?.od, v)
        }

        // -0.0: numeric equality above can't distinguish -0.0 from +0.0. The sign bit
        // is LOST through storage — SQLite's record format stores a REAL as an
        // INTEGER when the conversion is "lossless" (value == (double)(int64)value),
        // and -0.0 == 0 satisfies that check, so it comes back as integer 0 → +0.0.
        // This is SQLite storage behavior, not a Blackbird bug; documented here.
        let negZero = try await EdgeValDoubleModel.read(from: db, id: 0)
        XCTAssertEqual(negZero?.d.sign, .plus, "-0.0's sign bit is expected to be dropped by SQLite's integer-packing of REALs; if this fails with .minus, SQLite behavior changed")
        let rows = try await db.query("SELECT typeof(d) AS t FROM EdgeValDoubleModel WHERE id = 0")
        XCTAssertEqual(rows.first?["t"]?.stringValue, "real") // declared REAL; the integer packing is internal to the record format
        await db.close()
    }

    // SQLite has no NaN storage: binding NaN stores NULL. For an OPTIONAL Double
    // column that surfaces as nil after a round-trip.
    func testDoubleNaNInOptionalColumnBecomesNil() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await EdgeValDoubleModel(id: 1, d: 1.0, od: .nan).write(to: db)

        let read = try await EdgeValDoubleModel.read(from: db, id: 1)
        XCTAssertNotNil(read)
        XCTAssertNil(read?.od, "NaN should be stored as NULL by SQLite and read back as nil")

        let rows = try await db.query("SELECT typeof(od) AS t FROM EdgeValDoubleModel WHERE id = 1")
        XCTAssertEqual(rows.first?["t"]?.stringValue, "null")
        await db.close()
    }

    // For a NON-optional Double column (declared NOT NULL), NaN → NULL trips the
    // NOT NULL constraint, so the write throws rather than silently storing 0.
    // (Documented behavior: NaN cannot be persisted in a non-optional Double column.)
    func testDoubleNaNInNonOptionalColumnThrowsOnWrite() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await EdgeValDoubleModel(id: 1, d: 1.0, od: nil).write(to: db) // resolve schema first

        await AssertThrowsErrorAsync(try await EdgeValDoubleModel(id: 2, d: .nan, od: nil).write(to: db))

        // Infinity, unlike NaN, is a real IEEE value SQLite can store: it must round-trip.
        try await EdgeValDoubleModel(id: 3, d: .infinity, od: -.infinity).write(to: db)
        let inf = try await EdgeValDoubleModel.read(from: db, id: 3)
        XCTAssertEqual(inf?.d, .infinity)
        XCTAssertEqual(inf?.od, -.infinity)
        await db.close()
    }

    // MARK: Bool

    func testBoolRoundTrip() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await EdgeValBoolModel(id: 1, b: true).write(to: db)
        try await EdgeValBoolModel(id: 2, b: false).write(to: db)

        let t = try await EdgeValBoolModel.read(from: db, id: 1)
        let f = try await EdgeValBoolModel.read(from: db, id: 2)
        XCTAssertEqual(t?.b, true)
        XCTAssertEqual(f?.b, false)

        // Stored as INTEGER 1/0
        let rows = try await db.query("SELECT id, b, typeof(b) AS t FROM EdgeValBoolModel ORDER BY id")
        XCTAssertEqual(rows[0]["b"], .integer(1))
        XCTAssertEqual(rows[1]["b"], .integer(0))
        XCTAssertEqual(rows[0]["t"]?.stringValue, "integer")
        await db.close()
    }

    // MARK: Date

    func testDateEdgeValues() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        let subMillisecond = Date(timeIntervalSince1970: 1234567890.1234)
        let dates: [Date] = [.distantPast, .distantFuture, Date(timeIntervalSince1970: -1_000_000.5), subMillisecond, Date(timeIntervalSince1970: 0)]
        for (i, date) in dates.enumerated() {
            try await EdgeValDateModel(id: Int64(i), date: date, odate: date).write(to: db)
        }
        for (i, date) in dates.enumerated() {
            let read = try await EdgeValDateModel.read(from: db, id: Int64(i))
            XCTAssertNotNil(read)
            // Dates are stored as Double timeIntervalSince1970; comparing at 100µs
            // tolerance proves sub-millisecond precision survives the round-trip.
            XCTAssertEqual(read!.date.timeIntervalSince1970, date.timeIntervalSince1970, accuracy: 0.0001, "Date \(date) did not round-trip")
            XCTAssertEqual(read!.odate!.timeIntervalSince1970, date.timeIntervalSince1970, accuracy: 0.0001)
        }
        await db.close()
    }

    // MARK: String

    func testStringUnicodeEdgeCases() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        let strings: [String] = [
            "",                                     // empty
            "👩‍👩‍👧‍👦🏳️‍🌈🇺🇦",                         // multi-scalar emoji (ZWJ sequences, flags)
            "e\u{0301}\u{0327}",                    // combining marks
            "مرحبا بالعالم שלום",                   // right-to-left text
            "mixed רтл and ltr טקסט 123",           // bidirectional
            "'; DROP TABLE EdgeValStringModel; --", // quoting
        ]
        for (i, s) in strings.enumerated() {
            try await EdgeValStringModel(id: Int64(i), s: s).write(to: db)
        }
        for (i, s) in strings.enumerated() {
            let read = try await EdgeValStringModel.read(from: db, id: Int64(i))
            XCTAssertEqual(read?.s, s, "String \(s.debugDescription) did not round-trip")
            XCTAssertEqual(read.map { Array($0.s.utf8) }, Array(s.utf8), "UTF-8 bytes changed for \(s.debugDescription)")
        }
        await db.close()
    }

    func testStringOneMegabyte() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        let big = String(repeating: "abcdefghijklmnop", count: 65536) // 1 MiB of ASCII
        XCTAssertEqual(big.utf8.count, 1_048_576)
        try await EdgeValStringModel(id: 1, s: big).write(to: db)

        let read = try await EdgeValStringModel.read(from: db, id: 1)
        XCTAssertEqual(read?.s, big)
        await db.close()
    }

    // SQLite TEXT values may legally contain interior NUL bytes, and both sides of
    // the round-trip must handle them: binding must pass the exact UTF-8 byte length
    // (not -1/strlen, which truncates at the first NUL), and reading must use
    // sqlite3_column_bytes (not String(cString:), which also stops at NUL).
    // Regression guard: an earlier version of Blackbird.Value.bind used
    // `sqlite3_bind_text(..., -1, ...)` and the row reader used String(cString:),
    // silently truncating "a\0b" to "a".
    // Note: SQL length() can't verify this — it counts characters before the first
    // NUL by definition — so byte length is checked via CAST to BLOB.
    func testStringEmbeddedNULByteRoundTrip() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        let withNUL = "a\u{0}b"
        XCTAssertEqual(withNUL.utf8.count, 3)
        try await EdgeValStringModel(id: 1, s: withNUL).write(to: db)

        // All 3 bytes must reach storage:
        let rows = try await db.query("SELECT length(CAST(s AS BLOB)) AS bytelen, typeof(s) AS t FROM EdgeValStringModel WHERE id = 1")
        XCTAssertEqual(rows.first?["bytelen"]?.intValue, 3, "embedded NUL was truncated at bind time")
        XCTAssertEqual(rows.first?["t"]?.stringValue, "text")

        // ...and must all come back on read:
        let read = try await EdgeValStringModel.read(from: db, id: 1)
        XCTAssertEqual(read?.s, withNUL, "embedded NUL was truncated at read time")
        await db.close()
    }

    // MARK: Data

    func testDataEdgeCases() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        let blobs: [Data] = [
            Data(),                                  // empty
            Data([0x00]),                            // single NUL
            Data([0x61, 0x00, 0x62, 0x00, 0x63]),    // interior NULs (blobs, unlike text, must survive)
            Data([0xFF, 0xFE, 0x00, 0x01]),          // non-UTF-8 bytes
        ]
        for (i, blob) in blobs.enumerated() {
            try await EdgeValDataModel(id: Int64(i), data: blob).write(to: db)
        }
        for (i, blob) in blobs.enumerated() {
            let read = try await EdgeValDataModel.read(from: db, id: Int64(i))
            XCTAssertEqual(read?.data, blob, "Data \(Array(blob)) did not round-trip")
        }
        await db.close()
    }

    func testDataOneMegabyteRandom() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        var big = Data(capacity: 1_048_576)
        var rng = SystemRandomNumberGenerator()
        for _ in 0..<131_072 { withUnsafeBytes(of: rng.next() as UInt64) { big.append(contentsOf: $0) } }
        XCTAssertEqual(big.count, 1_048_576)

        try await EdgeValDataModel(id: 1, data: big).write(to: db)
        let read = try await EdgeValDataModel.read(from: db, id: 1)
        XCTAssertEqual(read?.data, big)
        await db.close()
    }

    // MARK: URL

    func testURLRoundTrips() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        let urls: [URL] = [
            URL(string: "https://example.com/path?query=1&other=two")!,
            URL(string: "https://example.com/page#fragment-here")!,
            URL(string: "https://example.com/a%20path%20with%20spaces/file%20name.txt")!,
            URL(string: "https://example.com/%E2%9C%93/%F0%9F%9A%80?q=%D8%B9%D8%B1%D8%A8%D9%8A#%C3%A9")!, // percent-encoded unicode
            URL(string: "file:///tmp/some%20file.txt")!,
            URL(string: "custom-scheme://host:1234/x?y=z")!,
        ]
        for (i, url) in urls.enumerated() {
            try await EdgeValURLModel(id: Int64(i), url: url, ourl: url).write(to: db)
        }
        for (i, url) in urls.enumerated() {
            let read = try await EdgeValURLModel.read(from: db, id: Int64(i))
            XCTAssertEqual(read?.url, url, "URL \(url) did not round-trip")
            XCTAssertEqual(read?.url.absoluteString, url.absoluteString)
            XCTAssertEqual(read?.ourl, url)
        }
        await db.close()
    }

    func testOptionalURLNilTransitions() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        let url = URL(string: "https://example.com/")!

        // nil → value
        try await EdgeValURLModel(id: 1, url: url, ourl: nil).write(to: db)
        var read = try await EdgeValURLModel.read(from: db, id: 1)
        XCTAssertNil(read?.ourl)

        var instance = read!
        instance.ourl = URL(string: "https://example.com/second")!
        try await instance.write(to: db)
        read = try await EdgeValURLModel.read(from: db, id: 1)
        XCTAssertEqual(read?.ourl?.absoluteString, "https://example.com/second")

        // value → nil
        instance = read!
        instance.ourl = nil
        try await instance.write(to: db)
        read = try await EdgeValURLModel.read(from: db, id: 1)
        XCTAssertNil(read?.ourl)

        let rows = try await db.query("SELECT ourl FROM EdgeValURLModel WHERE id = 1")
        XCTAssertEqual(rows.first?["ourl"], .null)
        await db.close()
    }

    // MARK: Optional columns

    func testOptionalColumnNilTransitions() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)

        try await EdgeValOptionalModel(id: 1, s: "hello", i: 42).write(to: db)

        // value → nil
        var instance = try await EdgeValOptionalModel.read(from: db, id: 1)!
        instance.s = nil
        instance.i = nil
        try await instance.write(to: db)

        var read = try await EdgeValOptionalModel.read(from: db, id: 1)
        XCTAssertNil(read?.s)
        XCTAssertNil(read?.i)
        var rows = try await db.query("SELECT s, i FROM EdgeValOptionalModel WHERE id = 1")
        XCTAssertEqual(rows.first?["s"], .null)
        XCTAssertEqual(rows.first?["i"], .null)

        // nil → value
        instance = read!
        instance.s = ""
        instance.i = 0
        try await instance.write(to: db)

        read = try await EdgeValOptionalModel.read(from: db, id: 1)
        XCTAssertEqual(read?.s, "")
        XCTAssertEqual(read?.i, 0)
        rows = try await db.query("SELECT s, i FROM EdgeValOptionalModel WHERE id = 1")
        XCTAssertEqual(rows.first?["s"], .text(""))
        XCTAssertEqual(rows.first?["i"], .integer(0))
        await db.close()
    }

    // MARK: Enum columns

    func testEnumColumnsRoundTripAllCases() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)

        var id: Int64 = 0
        for intCase in EdgeValEnumModel.IntCases.allCases {
            for stringCase in EdgeValEnumModel.StringCases.allCases {
                try await EdgeValEnumModel(id: id, intEnum: intCase, stringEnum: stringCase, optIntEnum: intCase, optStringEnum: stringCase).write(to: db)
                let read = try await EdgeValEnumModel.read(from: db, id: id)
                XCTAssertEqual(read?.intEnum, intCase)
                XCTAssertEqual(read?.stringEnum, stringCase)
                XCTAssertEqual(read?.optIntEnum, intCase)
                XCTAssertEqual(read?.optStringEnum, stringCase)
                id += 1
            }
        }

        // nil optional enums
        try await EdgeValEnumModel(id: id, intEnum: .zero, stringEnum: .alpha, optIntEnum: nil, optStringEnum: nil).write(to: db)
        let read = try await EdgeValEnumModel.read(from: db, id: id)
        XCTAssertNil(read?.optIntEnum)
        XCTAssertNil(read?.optStringEnum)
        await db.close()
    }

    // An unknown raw value in a NON-optional integer-enum column must throw a
    // decoding error, not crash.
    func testIntEnumUnknownRawValueThrows() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await EdgeValEnumModel(id: 1, intEnum: .one, stringEnum: .alpha, optIntEnum: nil, optStringEnum: nil).write(to: db)
        try await db.query("UPDATE EdgeValEnumModel SET intEnum = 99 WHERE id = 1")

        await AssertThrowsErrorAsync(_ = try await EdgeValEnumModel.read(from: db, id: 1))
        await db.close()
    }

    // Same for a NON-optional string-enum column.
    func testStringEnumUnknownRawValueThrows() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await EdgeValEnumModel(id: 1, intEnum: .one, stringEnum: .beta, optIntEnum: nil, optStringEnum: nil).write(to: db)
        try await db.query("UPDATE EdgeValEnumModel SET stringEnum = 'bogus-no-such-case' WHERE id = 1")

        await AssertThrowsErrorAsync(_ = try await EdgeValEnumModel.read(from: db, id: 1))
        await db.close()
    }

    // QUESTIONABLE BEHAVIOR (documented): an unknown raw value in an OPTIONAL enum
    // column does NOT throw — it silently decodes as nil. This comes from the
    // `Optional: RawRepresentable` retroactive conformance
    // (Sources/Blackbird/BlackbirdColumnTypes.swift:189-195), whose init(rawValue:)
    // maps unknown raw values to `.none` instead of failing, so the decoder's
    // invalidEnumValue guard (Sources/Blackbird/BlackbirdCodable.swift:133-147)
    // never trips for optionals. Data corruption is masked as nil rather than
    // surfaced as an error — inconsistent with the non-optional behavior above.
    func testOptionalEnumUnknownRawValueDecodesAsNil() async throws {
        let db = try Blackbird.Database(path: sqliteFilename)
        try await EdgeValEnumModel(id: 1, intEnum: .one, stringEnum: .beta, optIntEnum: .one, optStringEnum: .beta).write(to: db)
        try await db.query("UPDATE EdgeValEnumModel SET optIntEnum = 99, optStringEnum = 'bogus-no-such-case' WHERE id = 1")

        let read = try await EdgeValEnumModel.read(from: db, id: 1)
        XCTAssertNotNil(read, "optional-enum decode of unknown raw value should not fail the whole row read")
        XCTAssertNil(read?.optIntEnum, "unknown raw value in optional int-enum column currently decodes as nil (documented)")
        XCTAssertNil(read?.optStringEnum, "unknown raw value in optional string-enum column currently decodes as nil (documented)")
        await db.close()
    }

    // MARK: Blackbird.Value: SQLite literals

    func testFromSQLiteLiteralBasics() {
        XCTAssertEqual(Blackbird.Value.fromSQLiteLiteral("NULL"), .null)
        XCTAssertEqual(Blackbird.Value.fromSQLiteLiteral("'abc'"), .text("abc"))
        XCTAssertEqual(Blackbird.Value.fromSQLiteLiteral("''"), .text(""))
        XCTAssertEqual(Blackbird.Value.fromSQLiteLiteral("'it''s'"), .text("it's"))
        XCTAssertEqual(Blackbird.Value.fromSQLiteLiteral("''''"), .text("'"))
        XCTAssertEqual(Blackbird.Value.fromSQLiteLiteral("42"), .integer(42))
        XCTAssertEqual(Blackbird.Value.fromSQLiteLiteral("-9223372036854775808"), .integer(.min))
        XCTAssertEqual(Blackbird.Value.fromSQLiteLiteral("3.25"), .double(3.25))
        XCTAssertEqual(Blackbird.Value.fromSQLiteLiteral("X''"), .data(Data()))
        XCTAssertEqual(Blackbird.Value.fromSQLiteLiteral("X'DEADBEEF'"), .data(Data([0xDE, 0xAD, 0xBE, 0xEF])))

        // Non-literals must return nil, not crash:
        XCTAssertNil(Blackbird.Value.fromSQLiteLiteral("abc"))          // garbage
        XCTAssertNil(Blackbird.Value.fromSQLiteLiteral("'abc"))         // unterminated quote
        XCTAssertNil(Blackbird.Value.fromSQLiteLiteral(""))             // empty input
    }

    func testFromSQLiteLiteralAdversarial() {
        // These three inputs used to crash (odd-length hex read past the end of the
        // pair array; "'" and "X'" satisfied both the prefix and suffix checks with
        // overlapping characters, building invalid ranges). They must return nil.
        XCTAssertNil(Blackbird.Value.fromSQLiteLiteral("X'ABC'"))       // odd-length hex
        XCTAssertNil(Blackbird.Value.fromSQLiteLiteral("'"))            // overlapping quote prefix/suffix
        XCTAssertNil(Blackbird.Value.fromSQLiteLiteral("X'"))           // overlapping blob prefix/suffix

        // These near-miss inputs are handled safely today and must stay that way:
        XCTAssertNil(Blackbird.Value.fromSQLiteLiteral("X"))
        XCTAssertNil(Blackbird.Value.fromSQLiteLiteral("X'AB"))         // unterminated blob
        XCTAssertNotNil(Blackbird.Value.fromSQLiteLiteral("X'ZZ'"))     // non-hex pairs: currently .data(Data()) via compactMap
        XCTAssertEqual(Blackbird.Value.fromSQLiteLiteral("X'ZZ'"), .data(Data()))
    }

    func testSQLiteLiteralRoundTripsAllCases() {
        let values: [Blackbird.Value] = [
            .null,
            .integer(0), .integer(-1), .integer(.max), .integer(.min),
            .double(0.5), .double(-0.0), .double(.pi), .double(.greatestFiniteMagnitude), .double(.leastNonzeroMagnitude),
            .text(""), .text("it's a 'test'"), .text("emoji 🐦 and\nnewline"),
            .data(Data()), .data(Data([0x00])), .data(Data([0xDE, 0xAD, 0xBE, 0xEF])),
        ]
        for value in values {
            let literal = value.sqliteLiteral()
            let parsed = Blackbird.Value.fromSQLiteLiteral(literal)
            XCTAssertEqual(parsed, value, "sqliteLiteral()→fromSQLiteLiteral() failed for \(value): literal was \(literal)")
        }
    }

    // MARK: Blackbird.Value: comparisons

    func testValueComparisonAcrossMismatchedTypes() {
        // These mostly document coercion-based ordering; the essential assertion is
        // that no combination crashes.
        let all: [Blackbird.Value] = [.null, .integer(1), .double(1.5), .text("a"), .data(Data([0x01]))]
        for lhs in all {
            for rhs in all {
                _ = lhs < rhs // must not crash for any combination
            }
        }

        XCTAssertFalse(Blackbird.Value.null < .integer(1))   // null is never less-than
        XCTAssertFalse(Blackbird.Value.null < .null)
        XCTAssertTrue(Blackbird.Value.integer(1) < .integer(2))
        XCTAssertTrue(Blackbird.Value.integer(1) < .text("2"))    // rhs coerced to integer
        XCTAssertFalse(Blackbird.Value.integer(1) < .text("abc")) // non-numeric text coerces to 0
        XCTAssertTrue(Blackbird.Value.text("10") < .text("9"))    // lexicographic, not numeric
        XCTAssertTrue(Blackbird.Value.double(0.5) < .integer(1))
        XCTAssertTrue(Blackbird.Value.data(Data()) < .data(Data([0x01]))) // data compares by count
    }

    // MARK: Blackbird.Value: fromAny

    func testFromAnyConversions() throws {
        XCTAssertEqual(try Blackbird.Value.fromAny(nil), .null)
        XCTAssertEqual(try Blackbird.Value.fromAny(NSNull()), .null)

        // A Value passes through unchanged
        XCTAssertEqual(try Blackbird.Value.fromAny(Blackbird.Value.text("x")), .text("x"))
        XCTAssertEqual(try Blackbird.Value.fromAny(Blackbird.Value.null), .null)

        // Nested optionals unwrap
        let nestedSome: Int?? = 1
        let nestedNone: Int?? = Int??.some(nil)
        XCTAssertEqual(try Blackbird.Value.fromAny(nestedSome as Any?), .integer(1))
        XCTAssertEqual(try Blackbird.Value.fromAny(nestedNone as Any?), .null)

        XCTAssertEqual(try Blackbird.Value.fromAny(true), .integer(1))
        XCTAssertEqual(try Blackbird.Value.fromAny(false), .integer(0))
        XCTAssertEqual(try Blackbird.Value.fromAny(Int8(-5)), .integer(-5))
        XCTAssertEqual(try Blackbird.Value.fromAny(Int64.min), .integer(.min))
        XCTAssertEqual(try Blackbird.Value.fromAny(3.5), .double(3.5))
        XCTAssertEqual(try Blackbird.Value.fromAny("hello"), .text("hello"))
        XCTAssertEqual(try Blackbird.Value.fromAny(Substring("hello")), .text("hello"))
        XCTAssertEqual(try Blackbird.Value.fromAny(Data([1, 2])), .data(Data([1, 2])))

        let date = Date(timeIntervalSince1970: 1000.5)
        XCTAssertEqual(try Blackbird.Value.fromAny(date), .double(1000.5))

        let url = URL(string: "https://example.com/x?y=z")!
        XCTAssertEqual(try Blackbird.Value.fromAny(url), .text("https://example.com/x?y=z"))

        XCTAssertEqual(try Blackbird.Value.fromAny(EdgeValEnumModel.IntCases.negative), .integer(-5))
        XCTAssertEqual(try Blackbird.Value.fromAny(EdgeValEnumModel.StringCases.beta), .text("beta"))

        // Unsupported types must throw, not crash
        struct Unsupported { let x = 1 }
        XCTAssertThrowsError(try Blackbird.Value.fromAny(Unsupported()))
        XCTAssertThrowsError(try Blackbird.Value.fromAny([1, 2, 3]))
    }

    // MARK: Blackbird.Value: coercion matrix

    func testValueCoercionMatrix() {
        // .null: everything nil
        let null = Blackbird.Value.null
        XCTAssertNil(null.boolValue)
        XCTAssertNil(null.intValue)
        XCTAssertNil(null.int64Value)
        XCTAssertNil(null.doubleValue)
        XCTAssertNil(null.stringValue)
        XCTAssertNil(null.dataValue)

        // .integer
        let int = Blackbird.Value.integer(42)
        XCTAssertEqual(int.boolValue, true)
        XCTAssertEqual(int.intValue, 42)
        XCTAssertEqual(int.int64Value, 42)
        XCTAssertEqual(int.doubleValue, 42.0)
        XCTAssertEqual(int.stringValue, "42")
        XCTAssertEqual(int.dataValue, "42".data(using: .utf8))
        XCTAssertEqual(Blackbird.Value.integer(0).boolValue, false)

        // .double
        let dbl = Blackbird.Value.double(3.5)
        XCTAssertEqual(dbl.boolValue, true)
        XCTAssertEqual(dbl.intValue, 3) // truncates toward zero
        XCTAssertEqual(dbl.int64Value, 3)
        XCTAssertEqual(dbl.doubleValue, 3.5)
        XCTAssertEqual(dbl.stringValue, "3.5")
        XCTAssertEqual(dbl.dataValue, "3.5".data(using: .utf8))
        XCTAssertEqual(Blackbird.Value.double(-3.5).intValue, -3)

        // .text numeric
        let numText = Blackbird.Value.text("42")
        XCTAssertEqual(numText.boolValue, true)
        XCTAssertEqual(numText.intValue, 42)
        XCTAssertEqual(numText.int64Value, 42)
        XCTAssertEqual(numText.doubleValue, 42.0)
        XCTAssertEqual(numText.stringValue, "42")
        XCTAssertEqual(numText.dataValue, "42".data(using: .utf8))

        // .text non-numeric: boolValue is false (not nil) because Int(s) ?? 0
        let text = Blackbird.Value.text("abc")
        XCTAssertEqual(text.boolValue, false)
        XCTAssertNil(text.intValue)
        XCTAssertNil(text.int64Value)
        XCTAssertNil(text.doubleValue)
        XCTAssertEqual(text.stringValue, "abc")
        XCTAssertEqual(text.dataValue, "abc".data(using: .utf8))

        // .text decimal: intValue nil, boolValue false — inconsistent with .double(3.7)
        // (whose boolValue is true), but documented behavior
        let decText = Blackbird.Value.text("3.7")
        XCTAssertEqual(decText.boolValue, false)
        XCTAssertNil(decText.intValue)
        XCTAssertEqual(decText.doubleValue, 3.7)

        // .data holding UTF-8 numeric text
        let numData = Blackbird.Value.data("42".data(using: .utf8)!)
        XCTAssertEqual(numData.boolValue, true)
        XCTAssertEqual(numData.intValue, 42)
        XCTAssertEqual(numData.int64Value, 42)
        XCTAssertEqual(numData.doubleValue, 42.0)
        XCTAssertEqual(numData.stringValue, "42")
        XCTAssertEqual(numData.dataValue, "42".data(using: .utf8))

        // .data holding non-UTF-8 bytes: everything except dataValue nil
        let rawData = Blackbird.Value.data(Data([0xFF, 0xFE]))
        XCTAssertNil(rawData.boolValue)
        XCTAssertNil(rawData.intValue)
        XCTAssertNil(rawData.int64Value)
        XCTAssertNil(rawData.doubleValue)
        XCTAssertNil(rawData.stringValue)
        XCTAssertEqual(rawData.dataValue, Data([0xFF, 0xFE]))
    }
}
