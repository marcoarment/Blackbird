# CLAUDE.md

## Project overview

Blackbird is a Swift SQLite database wrapper and ORM by Marco Arment, licensed under MIT. It provides a model layer using Swift concurrency (`async`/`await`) and `Codable`, with zero external dependencies. It targets macOS 12+, iOS 15+, watchOS 8+, and tvOS 15+.

The core idea: define a struct conforming to `BlackbirdModel` with `@BlackbirdColumn` properties, and Blackbird handles table creation, automatic schema migrations, type-safe queries, caching, change observation, and SwiftUI integration — all backed by SQLite.

## Build and test

```bash
# Build
swift build

# Run tests
swift test
```

The project uses Swift 6 language mode (`swiftLanguageModes: [.v6]`). There is one library target (`Blackbird`) and one test target (`BlackbirdTests`).

## Project structure

```
Sources/Blackbird/
  Blackbird.swift                       # Root type, Value enum, lock utilities, Semaphore
  BlackbirdDatabase.swift               # Blackbird.Database — async SQLite wrapper
  BlackbirdModel.swift                  # BlackbirdModel protocol — struct-to-table mapping
  BlackbirdColumn.swift                 # @BlackbirdColumn property wrapper
  BlackbirdColumnTypes.swift            # Column type protocols (BlackbirdStorableAs*, enums)
  BlackbirdSchema.swift                 # Schema detection and automatic migrations
  BlackbirdCodable.swift                # Codable-based serialization to/from SQLite
  BlackbirdCodingKey.swift              # CodingKey helpers
  BlackbirdRow.swift                    # Row result type from queries
  BlackbirdModelStructuredQuerying.swift # Type-safe queries using key-paths (\.$column == value)
  BlackbirdModelSearch.swift            # Full-text search
  BlackbirdCache.swift                  # Built-in model caching layer
  BlackbirdChanges.swift                # Fine-grained change tracking (Combine publishers)
  BlackbirdSwiftUI.swift                # SwiftUI property wrappers (@BlackbirdLiveModels, etc.)
  BlackbirdObservation.swift            # Observation framework support
  BlackbirdPerformanceLogger.swift      # Performance logging

Tests/BlackbirdTests/
  BlackbirdTestModels.swift             # Test model definitions (TestModel, etc.)
  BlackbirdTestData.swift               # Test data/fixtures
  BlackbirdTests.swift                  # XCTest test cases
```

## Key concepts

- **BlackbirdModel**: Protocol for structs that map to SQLite tables. Columns are declared with `@BlackbirdColumn`. Primary keys, indexes, and unique indexes are defined via static properties using key-paths.
- **Blackbird.Database**: Async SQLite wrapper. Supports queries, transactions, and in-memory databases. The inner `Core` type is actor-isolated for thread safety.
- **Blackbird.Value**: Enum wrapping SQLite types (`.null`, `.integer`, `.double`, `.text`, `.data`).
- **Structured queries**: Type-safe WHERE clauses using key-paths (e.g., `\.$title == "Sports"`), compile-time checked.
- **Automatic migrations**: Schema changes are detected and applied at runtime — no manual migration code needed.
- **SwiftUI integration**: `@BlackbirdLiveModels`, `@BlackbirdLiveModel`, `@BlackbirdLiveQuery` provide auto-updating views.

## Code conventions

- Swift 6 strict concurrency. The codebase uses `Sendable`, `@unchecked Sendable` (with documented justification), actor isolation, and `nonisolated` where needed.
- All source files include the project's ASCII-art header with MIT license.
- Column names in the database use the Swift property names directly (via `Codable`/`CodingKeys`).
- SQL table names use `$T` as a placeholder in model queries, replaced at runtime with the actual table name.
- Tests use `@testable import Blackbird` and XCTest, with async test helpers (`AssertNoThrowAsync`, `AssertThrowsErrorAsync`).
- Commit messages are short, imperative or descriptive (e.g., "Fixed NULL-evaluation bugs in structured queries", "Add modify() for atomic Model read-modify-writes").
