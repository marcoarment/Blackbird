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
    static var table = Blackbird.Table(
        columns: [
            Blackbird.Column(name: "id",    type: .integer),
            Blackbird.Column(name: "title", type: .text),
            Blackbird.Column(name: "url",   type: .text),
            Blackbird.Column(name: "meta",  type: .text),
        ],
        indexes: [
            Blackbird.Index(columnNames: ["title"]),
        ]
    )

    let id: Int64
    var title: String
    var url: URL
    
    var nonColumn: String = ""
    
    init(id: Int64, title: String, url: URL, nonColumn: String) {
        self.id = id
        self.title = title
        self.url = url
        self.nonColumn = nonColumn
    }
    
    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        self.id = try container.decode(Int64.self, forKey: .id)
        self.title = try container.decode(String.self, forKey: .title)
        self.url = try container.decode(URL.self, forKey: .url)
    }
}

struct TestModelWithoutIDColumn: BlackbirdModel {
    static var table = Blackbird.Table(
        columns: [
            Blackbird.Column(name: "pk",    type: .integer),
            Blackbird.Column(name: "title", type: .text),
        ], primaryKeyColumnNames: [
            "pk",
        ]
    )

    var id: Int { pk }
    var pk: Int
    var title: String
}

struct TestModelWithDescription: BlackbirdModel {
    static var table = Blackbird.Table(
        columns: [
            Blackbird.Column(name: "id",    type: .integer),
            Blackbird.Column(name: "url",   type: .text, mayBeNull: true),
            Blackbird.Column(name: "title", type: .text),
            Blackbird.Column(name: "description",  type: .text),
        ],
        indexes: [
            Blackbird.Index(columnNames: ["title"]),
            Blackbird.Index(columnNames: ["url"]),
        ]
    )

    let id: Int
    var url: URL?
    var title: String
    var description: String
}

struct Post: BlackbirdModel {
    static var table = Blackbird.Table(
        columns: [
            Blackbird.Column(name: "id",    type: .integer),
            Blackbird.Column(name: "title", type: .text),
            Blackbird.Column(name: "url",   type: .text, mayBeNull: true),
            Blackbird.Column(name: "image", type: .data, mayBeNull: true),
        ]
    )

    let id: Int
    var title: String
    var url: URL?
    var image: Data?
}

struct TypeTest: BlackbirdModel {
    static var table = Blackbird.Table(
        columns: [
            Blackbird.Column(name: "id", type: .integer),

            Blackbird.Column(name: "typeIntNull", type: .integer, mayBeNull: true),
            Blackbird.Column(name: "typeIntNotNull", type: .integer, mayBeNull: false),

            Blackbird.Column(name: "typeTextNull", type: .text, mayBeNull: true),
            Blackbird.Column(name: "typeTextNotNull", type: .text, mayBeNull: false),

            Blackbird.Column(name: "typeDoubleNull", type: .double, mayBeNull: true),
            Blackbird.Column(name: "typeDoubleNotNull", type: .double, mayBeNull: false),

            Blackbird.Column(name: "typeDataNull", type: .data, mayBeNull: true),
            Blackbird.Column(name: "typeDataNotNull", type: .data, mayBeNull: false),
        ]
    )

    let id: Int64
    
    let typeIntNull: Int64?
    let typeIntNotNull: Int64

    let typeTextNull: String?
    let typeTextNotNull: String

    let typeDoubleNull: Double?
    let typeDoubleNotNull: Double

    let typeDataNull: Data?
    let typeDataNotNull: Data
}

struct MulticolumnPrimaryKeyTest: BlackbirdModel {
    static var table = Blackbird.Table(
        columns: [
            Blackbird.Column(name: "userID", type: .integer),
            Blackbird.Column(name: "feedID", type: .integer),
            Blackbird.Column(name: "episodeID", type: .integer),
            
            Blackbird.Column(name: "completed", type: .integer),
            Blackbird.Column(name: "deleted", type: .integer),
            Blackbird.Column(name: "progress", type: .integer),
        ],
        primaryKeyColumnNames: ["userID", "feedID", "episodeID"]
    )
    
    var id: String { get { "\(userID)-\(feedID)-\(episodeID)" } }

    let userID: Int64
    let feedID: Int64
    let episodeID: Int64
}

// MARK: - Schema change: Add primary-key column

struct SchemaChangeAddPrimaryKeyColumnInitial: BlackbirdModel {
    static var table = Blackbird.Table(
        name: "SchemaChangeAddPrimaryKeyColumn",
        columns: [
            Blackbird.Column(name: "userID", type: .integer),
            Blackbird.Column(name: "feedID", type: .integer),
            Blackbird.Column(name: "subscribed", type: .integer),
        ],
        primaryKeyColumnNames: ["userID", "feedID"]
    )

    var id: String { get { "\(userID)-\(feedID)" } }

    let userID: Int64
    let feedID: Int64
    let subscribed: Bool
}

struct SchemaChangeAddPrimaryKeyColumnChanged: BlackbirdModel {
    static var table = Blackbird.Table(
        name: "SchemaChangeAddPrimaryKeyColumn",
        columns: [
            Blackbird.Column(name: "userID", type: .integer),
            Blackbird.Column(name: "feedID", type: .integer),
            Blackbird.Column(name: "episodeID", type: .integer),
            Blackbird.Column(name: "subscribed", type: .integer),
        ],
        primaryKeyColumnNames: ["userID", "feedID", "episodeID"]
    )

    var id: String { get { "\(userID)-\(feedID)-\(episodeID)" } }

    let userID: Int64
    let feedID: Int64
    let episodeID: Int64
    let subscribed: Bool
}




// MARK: - Schema change: Add columns

struct SchemaChangeAddColumnsInitial: BlackbirdModel {
    static var table = Blackbird.Table(
        name: "SchemaChangeAddColumns",
        columns: [
            Blackbird.Column(name: "id",    type: .integer),
            Blackbird.Column(name: "title", type: .text),
        ]
    )

    let id: Int64
    var title: String
}

struct SchemaChangeAddColumnsChanged: BlackbirdModel {
    static var table = Blackbird.Table(
        name: "SchemaChangeAddColumns",
        columns: [
            Blackbird.Column(name: "id",    type: .integer),
            Blackbird.Column(name: "title", type: .text),
            Blackbird.Column(name: "description", type: .text),
            Blackbird.Column(name: "url",   type: .text, mayBeNull: true),
            Blackbird.Column(name: "art",   type: .data),
        ]
    )

    let id: Int64
    var title: String
    var description: String
    var url: URL?
    var art: Data
}

// MARK: - Schema change: Drop columns

struct SchemaChangeRebuildTableInitial: BlackbirdModel {
    static var table = Blackbird.Table(
        name: "SchemaChangeRebuild",
        columns: [
            Blackbird.Column(name: "id",    type: .integer),
            Blackbird.Column(name: "title", type: .text),
            Blackbird.Column(name: "flags", type: .integer),
        ],
        primaryKeyColumnNames: ["id", "title"]
    )

    let id: Int64
    var title: String
    var flags: Int
}

struct SchemaChangeRebuildTableChanged: BlackbirdModel {
    static var table = Blackbird.Table(
        name: "SchemaChangeRebuild",
        columns: [
            Blackbird.Column(name: "id",    type: .integer),
            Blackbird.Column(name: "title", type: .text),
            Blackbird.Column(name: "flags", type: .text),
            Blackbird.Column(name: "description", type: .text),
        ]
    )

    let id: Int64
    var title: String
    var flags: String
    var description: String
}

// MARK: - Schema change: Add index

struct SchemaChangeAddIndexInitial: BlackbirdModel {
    static var table = Blackbird.Table(
        name: "SchemaChangeAddIndex",
        columns: [
            Blackbird.Column(name: "id",    type: .integer),
            Blackbird.Column(name: "title", type: .text),
        ]
    )

    let id: Int64
    var title: String
}

struct SchemaChangeAddIndexChanged: BlackbirdModel {
    static var table = Blackbird.Table(
        name: "SchemaChangeAddIndex",
        columns: [
            Blackbird.Column(name: "id",    type: .integer),
            Blackbird.Column(name: "title", type: .text),
        ],
        indexes: [
            Blackbird.Index(columnNames: ["title"])
        ]
    )

    let id: Int64
    var title: String
}
