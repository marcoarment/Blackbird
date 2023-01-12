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
    static var indexes: [[ColumnKeyPath]] = [
        [ \.$title ]
    ]

    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var title: String
    @BlackbirdColumn var url: URL
    
    var nonColumn: String = ""
}

struct TestModelWithoutIDColumn: BlackbirdModel {
    static var primaryKeyPaths: [ColumnKeyPath] = [ \.$pk ]

    @BlackbirdColumn var pk: Int
    @BlackbirdColumn var title: String
}

struct TestModelWithDescription: BlackbirdModel {
    static var indexes: [[ColumnKeyPath]] = [
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
}

struct MulticolumnPrimaryKeyTest: BlackbirdModel {
    static var primaryKeyPaths: [ColumnKeyPath] = [ \.$userID, \.$feedID, \.$episodeID ]

    @BlackbirdColumn var userID: Int64
    @BlackbirdColumn var feedID: Int64
    @BlackbirdColumn var episodeID: Int64
}

// MARK: - Schema change: Add primary-key column

struct SchemaChangeAddPrimaryKeyColumnInitial: BlackbirdModel {
    static var tableName = "SchemaChangeAddPrimaryKeyColumn"
    static var primaryKeyPaths: [ColumnKeyPath] = [ \.$userID, \.$feedID ]

    @BlackbirdColumn var userID: Int64
    @BlackbirdColumn var feedID: Int64
    @BlackbirdColumn var subscribed: Bool
}

struct SchemaChangeAddPrimaryKeyColumnChanged: BlackbirdModel {
    static var tableName = "SchemaChangeAddPrimaryKeyColumn"
    static var primaryKeyPaths: [ColumnKeyPath] = [ \.$userID, \.$feedID, \.$episodeID ]

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
    static var primaryKeyPaths: [ColumnKeyPath] = [ \.$id, \.$title ]

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
    static var indexes: [[ColumnKeyPath]] = [
        [ \.$title ]
    ]

    @BlackbirdColumn var id: Int64
    @BlackbirdColumn var title: String
}
