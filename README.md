# Blackbird

A small, fast, lightweight SQLite database wrapper and model layer, based on modern Swift concurrency and `Codable`, with no other dependencies.

__See [Project Status](#project-status) before using this for anything!__

## BlackbirdModel

A protocol to store structs in an [SQLite](https://www.sqlite.org/) database, with compiler-checked key-paths for common operations.

```swift
struct Post: BlackbirdModel {
    @BlackbirdColumn var id: Int
    @BlackbirdColumn var title: String
    @BlackbirdColumn var url: URL?
    @BlackbirdColumn var image: Data?
}

let post = Post(id: 1, title: "What I had for breakfast")
try await post.write(to: db)

// Fetch by primary key
let anotherPost = try await Post.read(from: db, id: 2)

// Or with a WHERE condition, using compiler-checked key-paths:
let theSportsPost = try await Post.read(from: db, matching: \.$title == "Sports")

// Or use your own custom SQL:
let posts = try await Post.read(from: db, where: "title LIKE ? ORDER BY RANDOM()", "Sports%")

// Select only certain columns, with row dictionaries typed by key-path:
for row in try await Post.query(in: db, columns: [\.$id, \.$image], matching: \.$url != nil) {
    let postID = row[\.$id]       // returns Int
    let imageData = row[\.$image] // returns Data?
}

// Monitor for changes
let listener = Post.changePublisher(in: db).sink { change in
    print("Post IDs changed: \(change.primaryKeys ?? "all of them")")
}

// A model with a custom primary-key column:
struct CustomPrimaryKeyModel: BlackbirdModel {
    static var primaryKey: [BlackbirdColumnKeyPath] = [ \.$pk ]

    @BlackbirdColumn var pk: Int
    @BlackbirdColumn var title: String
}

// A model with indexes and a multicolumn primary key:
struct Post: BlackbirdModel {
    @BlackbirdColumn var id: Int
    @BlackbirdColumn var title: String
    @BlackbirdColumn var date: Date
    @BlackbirdColumn var isPublished: Bool
    @BlackbirdColumn var url: URL?
    @BlackbirdColumn var productID: Int

    static var primaryKey: [BlackbirdColumnKeyPath] = [ \.$id, \.$title ]

    static var indexes: [[BlackbirdColumnKeyPath]] = [
        [ \.$title ],
        [ \.$isPublished, \.$date ]
    ]

    static var uniqueIndexes: [[BlackbirdColumnKeyPath]] = [
        [ \.$productID ]
    ]
}
```

## Database

A lightweight async wrapper around [SQLite](https://www.sqlite.org/).

```swift
let db = try Blackbird.Database(path: "/tmp/whatever.sqlite")

// SELECT with structure
for row in try await db.query("SELECT id FROM posts WHERE state = ?", 1) {
    let id = row["id"]
    // ...
}

// Run direct queries
try await db.execute("UPDATE posts SET comments = NULL")

// Transactions with the actor isolated
try await db.transaction { core in
    try core.query("INSERT INTO posts VALUES (?, ?)", 16, "Sports!")
    try core.query("INSERT INTO posts VALUES (?, ?)", 17, "Dewey Defeats Truman")
}
```


## Project status

Blackbird is an __alpha__. It's brand new.

Nobody should be using it more than I do, and I've barely used it.

The API might change dramatically at any time.

Really, don't build anything against this yet.

Immediate to-do list:

* More tests, especially around performance, multicolumn primary keys, legacy change notifications, and any Obj-C sync-method deadlock potential  
* Actually start using Blackbird in [my app](https://overcast.fm/) to refine the API/conventions, find any edge-case bugs, and ensure Obj-C compatibility layer is useful enough
* More examples in the documentation

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
