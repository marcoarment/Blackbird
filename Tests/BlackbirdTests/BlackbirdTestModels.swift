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
//  BlackbirdTestModels.swift
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

import Foundation
@testable import Blackbird

struct TestModel: BlackbirdModel {
    static let indexes: [[BlackbirdColumnKeyPath]] = [
        [ \.$title ]
    ]

    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var title: String
    @BlackbirdColumn var url: URL
    
    var nonColumn: String = ""
}

struct TestModelWithCache: BlackbirdModel {
    static let indexes: [[BlackbirdColumnKeyPath]] = [
        [ \.$title ]
    ]
    
    static let cacheLimit: Int = 100

    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var title: String
    @BlackbirdColumn var url: URL
}


struct TestModelWithoutIDColumn: BlackbirdModel {
    static let primaryKey: [BlackbirdColumnKeyPath] = [ \.$pk ]

    @BlackbirdColumn var pk: Int
    @BlackbirdColumn var title: String
}

struct TestModelWithDescription: BlackbirdModel {
    static let cacheLimit: Int = 0

    static let indexes: [[BlackbirdColumnKeyPath]] = [
        [ \.$title ],
        [ \.$url ]
    ]

    @BlackbirdColumn var id: Int
    @BlackbirdColumn var url: URL?
    @BlackbirdColumn var title: String
    @BlackbirdColumn var description: String
}

struct TestCodingKeys: BlackbirdModel {
    enum CodingKeys: String, BlackbirdCodingKey {
        case id
        case title = "customTitle"
        case description = "d"
    }

    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var title: String
    @BlackbirdColumn var description: String
}

struct TestCustomDecoder: BlackbirdModel {
    @BlackbirdColumn var id: Int
    @BlackbirdColumn var name: String
    @BlackbirdColumn var thumbnail: URL

    enum CodingKeys: String, BlackbirdCodingKey {
        case id = "idStr"
        case name = "nameStr"
        case thumbnail = "thumbStr"
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)

        // Special-case handling for BlackbirdDefaultsDecoder:
        //  supplies a valid numeric string instead of failing on
        //  the empty string ("") returned by BlackbirdDefaultsDecoder
        
        if decoder is BlackbirdDefaultsDecoder {
            self.id = 0
        } else {
            let idStr = try container.decode(String.self, forKey: .id)
            guard let id = Int(idStr) else {
                throw DecodingError.dataCorruptedError(forKey: .id, in: container, debugDescription: "Expected numeric string")
            }
            self.id = id
        }

        self.name = try container.decode(String.self, forKey: .name)
        self.thumbnail = try container.decode(URL.self, forKey: .thumbnail)
    }
}


struct TypeTest: BlackbirdModel {
    @BlackbirdColumn var id: Int64
    
    @BlackbirdColumn var typeIntNull: Int64?
    @BlackbirdColumn var typeIntNotNull: Int64

    @BlackbirdColumn var typeTextNull: String?
    @BlackbirdColumn var typeTextNotNull: String

    @BlackbirdColumn var typeDoubleNull: Double?
    @BlackbirdColumn var typeDoubleNotNull: Double

    @BlackbirdColumn var typeDataNull: Data?
    @BlackbirdColumn var typeDataNotNull: Data
    
    enum RepresentableIntEnum: Int, BlackbirdIntegerEnum {
        typealias RawValue = Int
        
        case zero = 0
        case one = 1
        case two = 2
    }
    @BlackbirdColumn var typeIntEnum: RepresentableIntEnum
    @BlackbirdColumn var typeIntEnumNull: RepresentableIntEnum?
    @BlackbirdColumn var typeIntEnumNullWithValue: RepresentableIntEnum?

    enum RepresentableStringEnum: String, BlackbirdStringEnum {
        typealias RawValue = String

        case empty = ""
        case zero = "zero"
        case one = "one"
        case two = "two"
    }
    @BlackbirdColumn var typeStringEnum: RepresentableStringEnum
    @BlackbirdColumn var typeStringEnumNull: RepresentableStringEnum?
    @BlackbirdColumn var typeStringEnumNullWithValue: RepresentableStringEnum?

    enum RepresentableIntNonZero: Int, BlackbirdIntegerEnum {
        typealias RawValue = Int
        
        case one = 1
        case two = 2
    }
    @BlackbirdColumn var typeIntNonZeroEnum: RepresentableIntNonZero
    @BlackbirdColumn var typeIntNonZeroEnumWithDefault: RepresentableIntNonZero = .one
    @BlackbirdColumn var typeIntNonZeroEnumNull: RepresentableIntNonZero?
    @BlackbirdColumn var typeIntNonZeroEnumNullWithValue: RepresentableIntNonZero?

    enum RepresentableStringNonEmpty: String, BlackbirdStringEnum {
        typealias RawValue = String

        case one = "one"
        case two = "two"
    }
    @BlackbirdColumn var typeStringNonEmptyEnum: RepresentableStringNonEmpty
    @BlackbirdColumn var typeStringNonEmptyEnumWithDefault: RepresentableStringNonEmpty = .two
    @BlackbirdColumn var typeStringNonEmptyEnumNull: RepresentableStringNonEmpty?
    @BlackbirdColumn var typeStringNonEmptyEnumNullWithValue: RepresentableStringNonEmpty?
    
    @BlackbirdColumn var typeURLNull: URL?
    @BlackbirdColumn var typeURLNotNull: URL

    @BlackbirdColumn var typeDateNull: Date?
    @BlackbirdColumn var typeDateNotNull: Date
}

struct MulticolumnPrimaryKeyTest: BlackbirdModel {
    static let primaryKey: [BlackbirdColumnKeyPath] = [ \.$userID, \.$feedID, \.$episodeID ]

    @BlackbirdColumn var userID: Int64
    @BlackbirdColumn var feedID: Int64
    @BlackbirdColumn var episodeID: Int64
}

public struct TestModelWithOptionalColumns: BlackbirdModel {
    @BlackbirdColumn public var id: Int64
    @BlackbirdColumn public var date: Date
    @BlackbirdColumn public var name: String
    @BlackbirdColumn public var value: String?
    @BlackbirdColumn public var otherValue: Int?
    @BlackbirdColumn public var optionalDate: Date?
    @BlackbirdColumn public var optionalURL: URL?
    @BlackbirdColumn public var optionalData: Data?
}

public struct TestModelWithUniqueIndex: BlackbirdModel {
    public static let uniqueIndexes: [[BlackbirdColumnKeyPath]] = [
        [ \.$a, \.$b, \.$c ],
    ]

    @BlackbirdColumn public var id: Int64
    @BlackbirdColumn public var a: String
    @BlackbirdColumn public var b: Int
    @BlackbirdColumn public var c: Date
}

public struct TestModelForUpdateExpressions: BlackbirdModel {
    @BlackbirdColumn public var id: Int64
    @BlackbirdColumn public var i: Int
    @BlackbirdColumn public var d: Double
}

// MARK: - Schema change: Add primary-key column

struct SchemaChangeAddPrimaryKeyColumnInitial: BlackbirdModel {
    static let tableName = "SchemaChangeAddPrimaryKeyColumn"
    static let primaryKey: [BlackbirdColumnKeyPath] = [ \.$userID, \.$feedID ]

    @BlackbirdColumn var userID: Int64
    @BlackbirdColumn var feedID: Int64
    @BlackbirdColumn var subscribed: Bool
}

struct SchemaChangeAddPrimaryKeyColumnChanged: BlackbirdModel {
    static let tableName = "SchemaChangeAddPrimaryKeyColumn"
    static let primaryKey: [BlackbirdColumnKeyPath] = [ \.$userID, \.$feedID, \.$episodeID ]

    @BlackbirdColumn var userID: Int64
    @BlackbirdColumn var feedID: Int64
    @BlackbirdColumn var episodeID: Int64
    @BlackbirdColumn var subscribed: Bool
}



// MARK: - Schema change: Add columns

struct SchemaChangeAddColumnsInitial: BlackbirdModel {
    static let tableName = "SchemaChangeAddColumns"

    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var title: String
}

struct SchemaChangeAddColumnsChanged: BlackbirdModel {
    static let tableName = "SchemaChangeAddColumns"

    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var title: String
    @BlackbirdColumn var description: String
    @BlackbirdColumn var url: URL?
    @BlackbirdColumn var art: Data
}

// MARK: - Schema change: Drop columns

struct SchemaChangeRebuildTableInitial: BlackbirdModel {
    static let tableName = "SchemaChangeRebuild"
    static let primaryKey: [BlackbirdColumnKeyPath] = [ \.$id, \.$title ]

    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var title: String
    @BlackbirdColumn var flags: Int
}

struct SchemaChangeRebuildTableChanged: BlackbirdModel {
    static let tableName = "SchemaChangeRebuild"
    
    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var title: String
    @BlackbirdColumn var flags: String
    @BlackbirdColumn var description: String
}

// MARK: - Schema change: Add index

struct SchemaChangeAddIndexInitial: BlackbirdModel {
    static let tableName = "SchemaChangeAddIndex"

    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var title: String
}

struct SchemaChangeAddIndexChanged: BlackbirdModel {
    static let tableName = "SchemaChangeAddIndex"
    static let indexes: [[BlackbirdColumnKeyPath]] = [
        [ \.$title ]
    ]

    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var title: String
}

// MARK: - Invalid index definition

struct DuplicateIndexesModel: BlackbirdModel {
    static let indexes: [[BlackbirdColumnKeyPath]] = [
        [ \.$title ]
    ]

    static let uniqueIndexes: [[BlackbirdColumnKeyPath]] = [
        [ \.$title ]
    ]

    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var title: String
}

// MARK: - Full-text search

struct FTSModel: BlackbirdModel {
    static let fullTextSearchableColumns: FullTextIndex = [
        \.$title       : .text(weight: 3.0),
        \.$description : .text,
        \.$category    : .filterOnly,
    ]

    @BlackbirdColumn var id: Int
    @BlackbirdColumn var title: String
    @BlackbirdColumn var url: URL
    @BlackbirdColumn var description: String
    @BlackbirdColumn var keywords: String
    @BlackbirdColumn var category: Int
}

struct FTSModelAfterMigration: BlackbirdModel {
    static let tableName = "FTSModel"

    static let fullTextSearchableColumns: FullTextIndex = [
        \.$title       : .text(weight: 3.0),
        \.$description : .text,
        \.$category    : .filterOnly,
        \.$keywords    : .text(weight: 0.5),
    ]

    @BlackbirdColumn var id: Int
    @BlackbirdColumn var title: String
    @BlackbirdColumn var url: URL
    @BlackbirdColumn var description: String
    @BlackbirdColumn var keywords: String
    @BlackbirdColumn var category: Int
}

struct FTSModelAfterDeletion: BlackbirdModel {
    static let tableName = "FTSModel"

    @BlackbirdColumn var id: Int
    @BlackbirdColumn var title: String
    @BlackbirdColumn var url: URL
    @BlackbirdColumn var description: String
    @BlackbirdColumn var keywords: String
    @BlackbirdColumn var category: Int
}
