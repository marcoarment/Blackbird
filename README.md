# Blackbird

A small, fast, lightweight SQLite database wrapper and model layer, based on modern Swift concurrency and `Codable`, with no other dependencies.

## Project status

Blackbird is an __alpha__. The API might change dramatically at any time.

You shouldn't build anything against this yet. But it's close.

## BlackbirdModel

A protocol to store structs in the [SQLite](https://www.sqlite.org/)-powered [Blackbird.Database](#blackbird-database), with compiler-checked key-paths for common operations.

Here's how you define a table:

```swift
import Blackbird

struct Post: BlackbirdModel {
    @BlackbirdColumn var id: Int
    @BlackbirdColumn var title: String
    @BlackbirdColumn var url: URL?
}
```

That's it. No `CREATE TABLE`, no separate table-definition logic, no additional steps.

And __automatic migrations__. Want to add or remove columns or indexes, or start using more of Blackbird's features such as custom `enum` columns, unique indexes, or custom primary keys? Just change the code:

```swift
struct Post: BlackbirdModel {
    static var primaryKey: [BlackbirdColumnKeyPath] = [ \.$guid, \.$id ]

    static var indexes: [[BlackbirdColumnKeyPath]] = [
        [ \.$title ],
        [ \.$publishedDate, \.$format ],
    ]

    static var uniqueIndexes: [[BlackbirdColumnKeyPath]] = [
        [ \.$guid ],
    ]
    
    enum Format: Int, BlackbirdIntegerEnum {
        case markdown
        case html
    }
    
    @BlackbirdColumn var id: Int
    @BlackbirdColumn var guid: String
    @BlackbirdColumn var title: String
    @BlackbirdColumn var publishedDate: Date?
    @BlackbirdColumn var format: Format
    @BlackbirdColumn var url: URL?
    @BlackbirdColumn var image: Data?
}
```

…and Blackbird will automatically migrate the table to the new schema at runtime.

Write instances safely and easily to a [Blackbird.Database](#blackbird-database):

```swift
let post = Post(id: 1, title: "What I had for breakfast")
try await post.write(to: db)
```

Perform queries in many different ways, preferring structured queries using key-paths for compile-time checking, type safety, and convenience:

```swift
// Fetch by primary key
let post = try await Post.read(from: db, id: 2)

// Or with a WHERE condition, using compiler-checked key-paths:
let posts = try await Post.read(from: db, matching: \.$title == "Sports")

// Select custom columns, with row dictionaries typed by key-path:
for row in try await Post.query(in: db, columns: [\.$id, \.$image], matching: \.$url != nil) {
    let postID = row[\.$id]       // returns Int
    let imageData = row[\.$image] // returns Data?
}
```

SQL is never required, but it's always available:

```swift
try await Post.query(in: db, "UPDATE $T SET format = ? WHERE date < ?", .html, date)

let posts = try await Post.read(from: db, sqlWhere: "title LIKE ? ORDER BY RANDOM()", "Sports%")

for row in try await Post.query(in: db, "SELECT MAX(id) AS max FROM $T WHERE url = ?", url) {
    let maxID = row["max"]?.intValue
}
```

Monitor for row- and column-level changes with Combine:

```swift
let listener = Post.changePublisher(in: db).sink { change in
    print("Post IDs changed: \(change.primaryKeys ?? "all")")
    print(" Columns changed: \(change.columnNames ?? "all")")
}
```

Blackbird is designed for SwiftUI, offering async-loading, automatically-updating result wrappers:

```swift
struct RootView: View {
    // The database that all child views will automatically use
    var database = try! Blackbird.Database.inMemoryDatabase()

    var body: some View {
        PostListView()
        .environment(\.blackbirdDatabase, database)
    }
}

struct PostListView: View {
    // Async-loading, auto-updating array of matching instances
    @BlackbirdLiveModels({ try await Post.read(from: $0, orderBy: .ascending(\.$id)) }) var posts
    
    // Async-loading, auto-updating rows from a custom query
    @BlackbirdLiveQuery(tableName: "Post", { try await $0.query("SELECT MAX(id) AS max FROM Post") }) var maxID

    var body: some View {
        VStack {
            if posts.didLoad {
                List {
                    ForEach(posts.results) { post in
                        NavigationLink(destination: PostView(post: post.liveModel)) {
                            Text(post.title)
                        }
                    }
                }
            } else {
                ProgressView()
            }
        }
        .navigationTitle(maxID.didLoad ? "\(maxID.results.first?["max"]?.intValue ?? 0) posts" : "Loading…")
    }
}

struct PostView: View {
    // Auto-updating instance
    @BlackbirdLiveModel var post: Post?

    var body: some View {
        VStack {
            if let post {
                Text(post.title)
            }
        }
    }
}
```

## Blackbird.Database

A lightweight async wrapper around [SQLite](https://www.sqlite.org/) that can be used with or without [BlackbirdModel](#BlackbirdModel).

```swift
let db = try Blackbird.Database(path: "/tmp/db.sqlite")

// SELECT with parameterized queries
for row in try await db.query("SELECT id FROM posts WHERE state = ?", 1) {
    let id = row["id"]?.intValue
    // ...
}

// Run direct queries
try await db.execute("UPDATE posts SET comments = NULL")

// Transactions with synchronous queries
try await db.transaction { core in
    try core.query("INSERT INTO posts VALUES (?, ?)", 16, "Sports!")
    try core.query("INSERT INTO posts VALUES (?, ?)", 17, "Dewey Defeats Truman")
}
```

## Wishlist for future Swift-language capabilities

* __Static type reflection for cleaner schema detection:__ Swift currently has no way to reflect a type's properties without creating an instance — [Mirror](https://developer.apple.com/documentation/swift/mirror) only reflects property names and values of given instances. If the language adds static type reflection in the future, my schema detection wouldn't need to rely on a hack using a Decoder to generate empty instances.)

* __KeyPath to/from String, static reflection of a type's KeyPaths:__ With the abilities to get a type's available KeyPaths (without some [crazy hacks](https://forums.swift.org/t/getting-keypaths-to-members-automatically-using-mirror/21207)) and create KeyPaths from strings at runtime, many of my hacks using Codable could be replaced with KeyPaths, which would be cleaner and probably much faster.

* __Cleaner protocol name (`Blackbird.Model`):__ Swift protocols can't contain dots or be nested within another type.

* __Nested struct definitions inside protocols:__ Sure would make a lot of my "BlackbirdModel…" names shorter.

## FAQ

### why is it called blackbird

[The plane](https://en.wikipedia.org/wiki/Lockheed_SR-71_Blackbird), of course.

It's old, awesome, and ridiculously fast. Well, this database library is based on old, awesome tech (SQLite), and it's ridiculously fast.

(If I'm honest, though, it's mostly because it's a cool-ass plane. I don't even really care about planes, generally. Just that one.)

### you know there are lots of other things called that

Of course [there are](https://en.wikipedia.org/wiki/Blackbird). Who cares?

This is a database engine that'll be used by, at most, a handful of nerds. It doesn't matter what it's called.

I like unique names (rather than generic or descriptive names, like `Model` or `SwiftSQLite`) because they're easier to search for and harder to confuse with other types. So I wanted something memorable. I suppose I could've called it something like `ButtDB` — memorable! — but as I use it over the coming years, I wanted to type something cooler after all of my `struct` definitions.

### why didn't you just use [other SQLite library]

I like to write my own libraries.

My libraries can perfectly match my needs and the way I expect them to work. And if my needs or expectations change, I can change the libraries.

I also learn a great deal when writing them, exercising and improving my skills to benefit the rest of my work.

And when I write the libraries, I understand how everything works as I'm using them, therefore creating fewer bugs and writing more efficient software.

### you know [other SQLite library] is faster

I know. Ironic, considering that I named this one after the fastest plane.

Other Swift SQLite libraries can be faster by omitting much of Blackbird's reflection, abstraction, and key-path usage. Some use code-generation methods, which can be very fast but complicate the build and tooling. Others take less-abstracted approaches that make usage more complicated and error-prone.

I've never written an app that was too slow to read its database, but I've frequently struggled with maintenance of large, complex codebases.

Blackbird's goal is to achieve my ideal balance of ease-of-use and bug-avoidance, even though it's therefore not the fastest Swift SQLite library.

Phones keep getting faster, but a bug is a bug forever.
