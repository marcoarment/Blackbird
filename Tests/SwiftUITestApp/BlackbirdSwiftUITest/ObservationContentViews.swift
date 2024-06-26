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
//  ObservationContentViews.swift
//  Created by Marco Arment on 12/5/22.
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

// MARK: - @Observable

@available(macOS 14, iOS 17, tvOS 17, watchOS 10, *)
struct ContentViewObservation: View {
    @Environment(\.blackbirdDatabase) var database
    
    @State var posts = Post.QueryObserver { try await Post.read(from: $0, orderBy: .ascending(\.$id)) }
    @State var count = Post.QueryObserver { try await $0.query("SELECT COUNT(*) AS c FROM Post").first?["c"] ?? 0 }

    var body: some View {
        VStack {
            if let posts = posts.result {
                List {
                    ForEach(posts) { post in
                        NavigationLink(destination: PostViewObservation(post: post.observer)) {
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
        .navigationTitle(
            count.result != nil ? "\(count.result!.stringValue ?? "?") posts, db: \(database?.id ?? 0)" : "Loading…"
        )
        .onAppear {
            posts.bind(to: database)
            count.bind(to: database)
        }
    }
}

@available(macOS 14, iOS 17, tvOS 17, watchOS 10, *)
struct PostViewObservation: View {
    @Environment(\.blackbirdDatabase) var database
    @State var post: Post.Observer

    @State var title: String = ""

    var body: some View {
        VStack {
            if let instance = post.instance {
                Text("Title")
                .font(.system(.title))

                TextField("Title", text: $title)

                Button {
                    Task {
                        var post = instance
                        post.title = title
                        if let database { try await post.write(to: database) }
                    }
                } label: {
                    Text("Update")
                }
            } else {
                Text("Post not found")
            }
            
            PostViewTitleObservation(post: post)
        }
        .padding()
        .navigationTitle(post.instance?.title ?? "")
        .toolbar {
            ToolbarItem(placement: .navigationBarTrailing) {
                Button {
                    Task {
                        if let database, var post = post.instance {
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
                        if let database { try? await post.instance?.delete(from: database) }
                    }
                } label: {
                    Image(systemName: "trash")
                }
            }
        }
        .onAppear {
            post.bind(to: database)
        }
    }
}

@available(macOS 14, iOS 17, tvOS 17, watchOS 10, *)
struct PostViewTitleObservation: View {
    let post: Post.Observer
    
    var body: some View {
        Text("Bound title: \(post.instance?.title ?? "(nil)")")
    }
}


