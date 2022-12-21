//
//  BlackbirdSwiftUITestApp.swift
//  Created by Marco Arment on 12/5/22.
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

import SwiftUI
import Blackbird

struct Post: BlackbirdModel {
    static var table = Blackbird.Table(columns: [
        Blackbird.Column(name: "id", type: .integer),
        Blackbird.Column(name: "title", type: .text),
    ])
    
    var id: Int64
    var title: String
}

@main
struct BlackbirdSwiftUITestApp: App {

    // In-memory database
    var database: Blackbird.Database = try! Blackbird.Database.inMemoryDatabase(options: [.debugPrintEveryQuery, .debugPrintEveryReportedChange])
    
    // On-disk database
//    var database: Blackbird.Database = try! Blackbird.Database(path: "\(FileManager.default.temporaryDirectory.path)/blackbird-swiftui-test.sqlite", options: [.debugPrintEveryQuery, .debugPrintEveryReportedChange])

    var firstPost = Post(id: 1, title: "First!")
    
    var body: some Scene {
        WindowGroup {
            NavigationView {
                List {
                    Section {
                        NavigationLink {
                            ContentViewEnvironmentDB()
                        } label: {
                            Text("Model list")
                        }

                        NavigationLink {
                            PostViewEnvironmentDB(post: firstPost.liveModel)
                        } label: {
                            Text("Single-model updater")
                        }
                    } header: {
                        Text("Environment database")
                    }

                    Section {
                        NavigationLink {
                            ContentViewBoundDB(database: database)
                        } label: {
                            Text("Model list")
                        }
                    } header: {
                        Text("Bound database")
                    }
                }
            }
            .environment(\.blackbirdDatabase, database)
            .onAppear {
                Task {
                    print("DB path: \(database.path ?? "in-memory")")

                    // Iterative version:
//                    try await firstPost.write(to: database)
//                    for _ in 0..<5 { try! await Post(id: TestData.randomInt64(), title: TestData.randomTitle).write(to: database) }

                    // Transaction version:
                    try await database.transaction { core in
                        try firstPost.writeIsolated(to: database, core: core)
                        for _ in 0..<5 { try! Post(id: TestData.randomInt64(), title: TestData.randomTitle).writeIsolated(to: database, core: core) }
                    }
                }
            }
        }
    }
}
