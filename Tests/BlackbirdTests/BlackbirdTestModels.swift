//
//  BlackbirdTestModels.swift
//  Created by Marco Arment on 11/20/22.
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
@testable import Blackbird

struct TestModel: BlackbirdModel {
    static var indexes: [[BlackbirdColumnKeyPath]] = [
        [ \.$title ]
    ]
    
    static var enableCaching: Bool = false

    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var title: String
    @BlackbirdColumn var url: URL
    
    var nonColumn: String = ""
}

struct TestModelWithoutIDColumn: BlackbirdModel {
    static var primaryKey: [BlackbirdColumnKeyPath] = [ \.$pk ]

    @BlackbirdColumn var pk: Int
    @BlackbirdColumn var title: String
}

struct TestModelWithDescription: BlackbirdModel {
    static var enableCaching = false

    static var indexes: [[BlackbirdColumnKeyPath]] = [
        [ \.$title ],
        [ \.$url ]
    ]

    @BlackbirdColumn var id: Int
    @BlackbirdColumn var url: URL?
    @BlackbirdColumn var title: String
    @BlackbirdColumn var description: String
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
        case zero = 0
        case one = 1
        case two = 2
    }
    @BlackbirdColumn var typeIntEnum: RepresentableIntEnum
    @BlackbirdColumn var typeIntEnumNull: RepresentableIntEnum?
    @BlackbirdColumn var typeIntEnumNullWithValue: RepresentableIntEnum?

    enum RepresentableStringEnum: String, BlackbirdStringEnum {
        case empty = ""
        case zero = "zero"
        case one = "one"
        case two = "two"
    }
    @BlackbirdColumn var typeStringEnum: RepresentableStringEnum
    @BlackbirdColumn var typeStringEnumNull: RepresentableStringEnum?
    @BlackbirdColumn var typeStringEnumNullWithValue: RepresentableStringEnum?

    enum RepresentableIntNonZero: Int, BlackbirdIntegerEnum {
        case one = 1
        case two = 2
    }
    @BlackbirdColumn var typeIntNonZeroEnum: RepresentableIntNonZero
    @BlackbirdColumn var typeIntNonZeroEnumWithDefault: RepresentableIntNonZero = .one
    @BlackbirdColumn var typeIntNonZeroEnumNull: RepresentableIntNonZero?
    @BlackbirdColumn var typeIntNonZeroEnumNullWithValue: RepresentableIntNonZero?

    enum RepresentableStringNonEmpty: String, BlackbirdStringEnum {
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
    static var primaryKey: [BlackbirdColumnKeyPath] = [ \.$userID, \.$feedID, \.$episodeID ]

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
    public static var uniqueIndexes: [[BlackbirdColumnKeyPath]] = [
        [ \.$a, \.$b, \.$c ],
    ]

    @BlackbirdColumn public var id: Int64
    @BlackbirdColumn public var a: String
    @BlackbirdColumn public var b: Int
    @BlackbirdColumn public var c: Date
}

// MARK: - Schema change: Add primary-key column

struct SchemaChangeAddPrimaryKeyColumnInitial: BlackbirdModel {
    static var tableName = "SchemaChangeAddPrimaryKeyColumn"
    static var primaryKey: [BlackbirdColumnKeyPath] = [ \.$userID, \.$feedID ]

    @BlackbirdColumn var userID: Int64
    @BlackbirdColumn var feedID: Int64
    @BlackbirdColumn var subscribed: Bool
}

struct SchemaChangeAddPrimaryKeyColumnChanged: BlackbirdModel {
    static var tableName = "SchemaChangeAddPrimaryKeyColumn"
    static var primaryKey: [BlackbirdColumnKeyPath] = [ \.$userID, \.$feedID, \.$episodeID ]

    @BlackbirdColumn var userID: Int64
    @BlackbirdColumn var feedID: Int64
    @BlackbirdColumn var episodeID: Int64
    @BlackbirdColumn var subscribed: Bool
}



// MARK: - Schema change: Add columns

struct SchemaChangeAddColumnsInitial: BlackbirdModel {
    static var tableName = "SchemaChangeAddColumns"

    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var title: String
}

struct SchemaChangeAddColumnsChanged: BlackbirdModel {
    static var tableName = "SchemaChangeAddColumns"

    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var title: String
    @BlackbirdColumn var description: String
    @BlackbirdColumn var url: URL?
    @BlackbirdColumn var art: Data
}

// MARK: - Schema change: Drop columns

struct SchemaChangeRebuildTableInitial: BlackbirdModel {
    static var tableName = "SchemaChangeRebuild"
    static var primaryKey: [BlackbirdColumnKeyPath] = [ \.$id, \.$title ]

    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var title: String
    @BlackbirdColumn var flags: Int
}

struct SchemaChangeRebuildTableChanged: BlackbirdModel {
    static var tableName = "SchemaChangeRebuild"
    
    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var title: String
    @BlackbirdColumn var flags: String
    @BlackbirdColumn var description: String
}

// MARK: - Schema change: Add index

struct SchemaChangeAddIndexInitial: BlackbirdModel {
    static var tableName = "SchemaChangeAddIndex"

    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var title: String
}

struct SchemaChangeAddIndexChanged: BlackbirdModel {
    static var tableName = "SchemaChangeAddIndex"
    static var indexes: [[BlackbirdColumnKeyPath]] = [
        [ \.$title ]
    ]

    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var title: String
}
