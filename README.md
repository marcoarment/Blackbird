# Blackbird

A small, fast, lightweight SQLite database wrapper and model layer, based on modern Swift concurrency and `Codable`, with no other dependencies.

__See [Project Status](#project-status) before using this for anything!__

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

## BlackbirdModel

A model protocol, using Swift's `Codable`, stored in a `Database`.

```swift
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

let post = Post(id: 1, title: "What I had for breakfast")
try await post.write(to: db)

// Fetch by primary key
let anotherPost = try await Post.read(from: db, id: 2)

// Or with a WHERE query, parameterized with SQLite data types
let theSportsPost = try await Post.read(from: db, where: "title = ?", "Sports")

// Monitor for changes
let listener = Post.changePublisher(in: db).sink { changedPrimaryKeys in
    print("Post IDs changed: \(changedPrimaryKeys ?? "all of them")")
}
```

## Project status

Blackbird is an __alpha at best__. It's brand new.

Nobody should be using it more than I do, and I've barely used it.

The API might change dramatically at any time.

Really, don't build anything against this yet.

Immediate to-do list:

* More tests, especially around performance, multicolumn primary keys, legacy change notifications, and any Obj-C sync-method deadlock potential  
* Actually start using Blackbird in [my app](https://overcast.fm/) to refine the API/conventions, find any edge-case bugs, and ensure Obj-C compatibility layer is useful enough
* More examples in the documentation
* Query-performance metrics (@siracusa)

## Wishlist for future Swift-language capabilities

* __Type reflection for automatic/cleaner schema definitions:__ Swift currently has no way to reflect a type's properties — [Mirror](https://developer.apple.com/documentation/swift/mirror) only reflects property names and values of given instances. If the language adds type reflection in the future, I'd love to add optional automatic table definitions that didn't require repeating the column names and types in my `static var table = …` definition. (Could also be based on property wrappers, e.g. `@Column` and `@IndexedColumn`.)

    Ideally, any future language capabilities regarding type reflection would also include the default values of each property if set.

* __Cleaner protocol name (`Blackbird.Model`):__ Swift protocols can't contain dots or be nested within another type.

## FAQ

### why is it called blackbird

[The plane](https://en.wikipedia.org/wiki/Lockheed_SR-71_Blackbird), of course.

It's old, awesome, and ridiculously fast. Well, this database library is based on old, awesome tech (SQLite), and it's ridiculously fast.

(If I'm honest, though, it's mostly because it's a cool-ass plane. I don't even really care about planes, generally. Just that one.)

### you know there are lots of other things called that

Of course [there are](https://en.wikipedia.org/wiki/Blackbird). Who cares?

This is a database engine that'll be used by, at most, a handful of nerds. It doesn't matter what it's called.

I like unique names (rather than generic or descriptive names, like `Model` or `SwiftSQLite`) because they're easier to search for and harder to confuse with other types. So I wanted something memorable. I suppose I could've called it something like `ButtDB` — memorable! — but as I use it over the coming years, I wanted to type something cooler after all of my `struct` definitions.

### why didn't you just use [other SQLite-based Swift library]

I like to write my own libraries.

My libraries can perfectly match my needs and the way I expect them to work. And if my needs or expectations change, I can change the libraries.

I also learn a great deal when writing them, exercising and improving my skills to benefit the rest of my work.

And when I write the libraries, I understand how everything works as I'm using them, therefore creating fewer bugs and writing more efficient software.

### why doesn't it abstract more of SQLite with more compile-time restrictions or query builders or dot-chaining or

I didn't want to get too far from using SQLite.

This is for people who want the CRUD basics taken care of, but might sometimes want to write their own custom `SELECT` or `UPDATE` queries or control their own indexes without fighting the library or incurring unnecessary overhead.

Also, dot-chaining sucks.
