//
//  ContentView.swift
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

// MARK: - @BlackbirdFetch with @Environment(\.blackbirdDatabase)

struct ContentViewEnvironmentDB: View {
    @Environment(\.blackbirdDatabase) var database

    @BlackbirdLiveModels({ try await Post.read(from: $0, where: "1 ORDER BY id") }) var posts

    @BlackbirdLiveQuery(tableName: "Post", { try await $0.query("SELECT COUNT(*) FROM Post") }) var count

    var body: some View {
        VStack {
            if posts.didLoad {
                List {
                    ForEach(posts.results) { post in
                        NavigationLink(destination: PostViewEnvironmentDB(post: post.liveModel)) {
                            Text(post.title)
                        }
                        .transition(.move(edge: .leading))
                    }
                }
                .animation(.default, value: posts)
            } else {
                ProgressView()
            }
        }
        .toolbar {
            ToolbarItem(placement: .navigationBarTrailing) {
                Button {
                    Task { if let db = database { try! await Post(id: TestData.randomInt64(), title: TestData.randomTitle).write(to: db) } }
                } label: {
                    Image(systemName: "plus")
                }
            }
        }
        .navigationTitle(count.didLoad ? "\(count.results.first?["COUNT(*)"]?.stringValue ?? "?") posts, db: \(database?.id ?? 0)" : "Loading…")
    }
}

struct PostViewEnvironmentDB: View {
    @Environment(\.blackbirdDatabase) var database
    @BlackbirdLiveModel var post: Post?

    @State var title: String = ""

    var body: some View {
        VStack {
            if let post {
                Text("Title")
                .font(.system(.title))

                TextField("Title", text: $title)

                Button {
                    Task {
                        var post = post
                        post.title = title
                        if let database { try await post.write(to: database) }
                    }
                } label: {
                    Text("Update")
                }
            } else {
                Text("Post not found")
            }
        }
        .padding()
        .navigationTitle(post?.title ?? "")
        .toolbar {
            ToolbarItem(placement: .navigationBarTrailing) {
                Button {
                    Task {
                        if let database, var post {
                            post.title = "✏️ \(post.title)"
                            try? await post.write(to: database)
                        }
                    }
                } label: {
                    Image(systemName: "scribble")
                }
            }

            ToolbarItem(placement: .navigationBarTrailing) {
                Button {
                    Task {
                        if let database { try? await post?.delete(from: database) }
                    }
                } label: {
                    Image(systemName: "trash")
                }
            }
        }
        .onChange(of: post) { newValue in
            title = newValue?.title ?? ""
        }
        .onAppear {
            if let post { title = post.title }
        }
    }
}


// MARK: - Locally bound database with .QueryUpdater

struct ContentViewBoundDB: View {
    @State var database: Blackbird.Database
    @State var posts = Post.LiveResults()
    var postsUpdater = Post.ArrayUpdater()

    var body: some View {
        VStack {
            if posts.didLoad {
                List {
                    ForEach(posts.results) { post in
                        NavigationLink(destination: PostViewBoundDB(database: $database, post: post)) {
                            Text(post.title)
                        }
                        .transition(.move(edge: .leading))
                    }
                }
                .animation(.default, value: posts)
            } else {
                ProgressView()
            }
        }
        .toolbar {
            ToolbarItem(placement: .navigationBarTrailing) {
                Button {
                    Task { try? await Post(id: TestData.randomInt64(), title: TestData.randomTitle).write(to: database) }
                } label: { Image(systemName: "plus") }
            }
        }
        .onAppear {
            postsUpdater.bind(from: database, to: $posts) { try await Post.read(from: $0, where: "1 ORDER BY id") }
        }
    }
}


struct PostViewBoundDB: View {
    @Binding var database: Blackbird.Database
    
    @State var post: Post?
    var postUpdater = Post.InstanceUpdater()
    @State var didLoad = false
    
    @State var title: String = ""

    var body: some View {
        VStack {
            if didLoad {
                if let post {
                    Text("Title")
                    .font(.system(.title))
                    
                    TextField("Title", text: $title)
                    
                    Button {
                        Task {
                            var post = post
                            post.title = title
                            try await post.write(to: database)
                        }
                    } label: {
                        Text("Update")
                    }
                } else {
                    Text("Post not found")
                }
            } else {
                ProgressView()
            }
        }
        .padding()
        .navigationTitle(post?.title ?? "")
        .toolbar {
            ToolbarItem(placement: .navigationBarTrailing) {
                Button {
                    Task { try? await post?.delete(from: database) }
                } label: {
                    Image(systemName: "trash")
                }
            }
        }
        .onAppear {
            if let post {
                title = post.title
                postUpdater.bind(from: database, to: $post, didLoad: $didLoad, id: post.id)
            }
        }
    }
}
