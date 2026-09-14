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
//  BlackbirdModelSearch.swift
//  Created by Marco Arment on 10/23/23.
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
import SQLite3

/// Defines a searchable field for a full-text index in a ``BlackbirdModel/fullTextSearchableColumns`` dictionary.
public struct BlackbirdModelFullTextSearchableColumn: Equatable, Hashable, Sendable {

    /// This column should be indexed as a text column, and can be used with `.match()` operations, with the default weight of `1.0`.
    public static var text: Self { .init(weight: 1.0, indexed: true) }

    /// This column should be indexed as a text column, and can be used with `.match()` operations.
    /// - Parameters:
    ///   - weight: The weight of this column for search relevance, relative to other indexed columns. Default is `1.0`.
    public static func text(weight: Double = 1.0) -> Self { .init(weight: weight, indexed: true) }

    /// This column should be present in the index for filtering with `WHERE` clauses, but not indexed as text or usable with `.match()` operations.
    public static var filterOnly: Self { .init(weight: 0, indexed: false) }

    internal let weight: Double
    internal let indexed: Bool

    private init(weight: Double, indexed: Bool) {
        self.weight = weight
        self.indexed = indexed
    }

    public static func == (lhs: Self, rhs: Self) -> Bool { return lhs.weight == rhs.weight && lhs.indexed == rhs.indexed }
    public func hash(into hasher: inout Hasher) {
        hasher.combine(weight)
        hasher.combine(indexed)
    }
}

/// Options for full-text-search queries.
public struct BlackbirdModelSearchOptions<T: BlackbirdModel>: Sendable {
    public enum HighlightMode: Sendable {
        /// Do not generate highlights.
        case none
        
        /// Generate copies of the source text that highlight occurrences of the search terms.
        ///
        /// ## Example
        /// Given:
        /// * Source text:  `"a b c d e d c b a"`
        /// * Search query: `"c"`
        /// * A prefix of `"<b>"`
        /// * A suffix of `"</b>"`
        ///
        /// The resulting highlight would be:
        ///
        /// `"a b <b>c</b> d e d <b>c</b> b a"`
        case generate(prefix: String, suffix: String)
    }

    public enum SnippetMode: Sendable {
        /// Do not generate snippets.
        case none
        
        /// Generate snippets that highlight the best match of the given search terms in the source text.
        ///
        /// ## Example
        /// Given:
        /// * Source text:  `"a b c d e"`
        /// * Search query: `"c"`
        /// * 3 context words
        /// * A prefix of `"<b>"`
        /// * A suffix of `"</b>"`
        /// * An ellipsis of `"…"`
        ///
        /// The resulting snippet would be:
        ///
        /// `"…b <b>c</b> d…"`
        case generate(contextWords: Int, prefix: String, suffix: String, ellipsis: String)
    }
    
    /// Whether and how to highlight search terms in results.
    let highlights: HighlightMode
    
    /// Whether and how to generate snippets showing search terms in results.
    let snippets: SnippetMode
    
    /// If `true`, every instance in search results will be prefetched and available from the ``BlackbirdModelSearchResult/preloadedInstance`` property.
    let preloadInstances: Bool
    
    /// If set, relevance scores are multiplied by the numeric value in this column for search ranking. Results with higher values in this column will be ranked higher. This column must be in the full-text index.
    ///
    /// Can be used simultaneously with ``scoreMultiple``. Scores will be multiplied by both values.
    let scoreMultipleColumn: T.BlackbirdColumnKeyPath?
    
    /// If set, all result scores in this query are multiplied by this value. Useful when merging the results of multiple searches, such as when performing secondary searches for spelling-corrected queries.
    ///
    /// Can be used simultaneously with ``scoreMultipleColumn``. Scores will be multiplied by both values.
    let scoreMultiple: Double
    
    let orderBy: [BlackbirdModelOrderClause<T>]
    
    /// The default options: generate highlights and snippets, preload instances, and use only relevance-based ranking.
    public static var `default`: Self { .init() }

    public init(highlights: HighlightMode = .generate(prefix: "<b>", suffix: "</b>"), snippets: SnippetMode = .generate(contextWords: 7, prefix: "<b>", suffix: "</b>", ellipsis: "…"), preloadInstances: Bool = true, scoreMultiple: Double = 1.0, scoreMultipleColumn: T.BlackbirdColumnKeyPath? = nil, orderBy: BlackbirdModelOrderClause<T> ...) {
        self.highlights = highlights
        self.snippets = snippets
        self.preloadInstances = preloadInstances
        self.scoreMultiple = scoreMultiple
        self.scoreMultipleColumn = scoreMultipleColumn
        self.orderBy = orderBy
    }

    public init(highlights: HighlightMode = .generate(prefix: "<b>", suffix: "</b>"), snippets: SnippetMode = .generate(contextWords: 7, prefix: "<b>", suffix: "</b>", ellipsis: "…"), preloadInstances: Bool = true, scoreMultiple: Double = 1.0, scoreMultipleColumn: T.BlackbirdColumnKeyPath? = nil, orderBy: [BlackbirdModelOrderClause<T>]) {
        self.highlights = highlights
        self.snippets = snippets
        self.preloadInstances = preloadInstances
        self.scoreMultiple = scoreMultiple
        self.scoreMultipleColumn = scoreMultipleColumn
        self.orderBy = orderBy
    }
}

/// A matching model from a full-text search query, with snippets to highlight the query in the source text.
public struct BlackbirdModelSearchResult<T: BlackbirdModel>: Identifiable, Sendable {
    /// Intended only for `Identifiable` conformance, not external use.
    public var id: Blackbird.Value { rowid }
    
    /// The score of this item within the search that generated it. Results with higher values were ranked earlier and considered more relevant than those with lower values.
    ///
    /// Values are only meaningful as relative relevance within their search, not as an absolute scale or indicator of anything outside the context of the search that generated them.
    public let score: Double
    
    private let highlights: Blackbird.ModelRow<T>?
    private let highlightMode: BlackbirdModelSearchOptions<T>.HighlightMode
    private let snippets: Blackbird.ModelRow<T>?
    private let snippetMode: BlackbirdModelSearchOptions<T>.SnippetMode
    private let rowid: Blackbird.Value
    
    
    /// Merge results from multiple searches.
    /// - Parameter results: A list of result arrays.
    /// - Returns: A single result array containing all elements of the input arrays, with duplicates removed, and with each element bearing its highest score among all occurrences in the input arrays.
    public static func merge(results: [Self]...) -> [Self] {
        var allResults: [Self] = []
        for resultSet in results { allResults.append(contentsOf: resultSet) }
        
        // Sort by rowid, then highest-scoring version
        allResults.sort { a, b in
            if a.rowid == b.rowid { return a.score > b.score }
            return a.rowid > b.rowid
        }

        // Create array with only the first copy of each rowid, which will now have the highest score
        var mergedResults: [Self] = []
        var rowids = Set<Blackbird.Value>()
        for result in allResults {
            let (inserted, _) = rowids.insert(result.rowid)
            if inserted { mergedResults.append(result) }
        }

        // sort by score
        mergedResults.sort { $0.score > $1.score }

        return mergedResults
    }

    /// The preloaded instance for this search result if ``BlackbirdModelSearchOptions`` specified `preloadInstances` for the search query.
    public let preloadedInstance: T?

    internal init(highlights: Blackbird.ModelRow<T>?, highlightMode: BlackbirdModelSearchOptions<T>.HighlightMode, snippets: Blackbird.ModelRow<T>?, snippetMode: BlackbirdModelSearchOptions<T>.SnippetMode, rowid: Blackbird.Value, score: Double, preloadedInstance: T?) {
        self.highlights = highlights
        self.highlightMode = highlightMode
        self.snippets = snippets
        self.snippetMode = snippetMode
        self.rowid = rowid
        self.score = score
        self.preloadedInstance = preloadedInstance
    }

    /// The full model instance for this search result.
    /// - Parameter database: The ``Blackbird/Database`` to read from.
    /// - Returns: The specified instance, or `nil` if it no longer exists in the database.
    public func instance(from database: Blackbird.Database) async throws -> T? {
        if let preloadedInstance { return preloadedInstance }
        return try await database.core.performGated { try instance(from: $0) }
    }
    
    /// Synchronous version of ``instance(from:)`` for use in database transactions.
    public func instance(from core: isolated Blackbird.Database.Core) throws -> T? {
        if let preloadedInstance { return preloadedInstance }
        return try T.read(from: core, sqlWhere: "rowid = ?", arguments: [rowid]).first
    }
    
    /// A subset of columns from this search result's model row.
    /// - Parameters:
    ///   - database: The ``Blackbird/Database`` to read from.
    ///   - columns: The column key-paths to select.
    /// - Returns: A ``Blackbird/ModelRow`` of the model's type with only the specified columns present.
    public func row(from database: Blackbird.Database, columns: [T.BlackbirdColumnKeyPath]) async throws -> Blackbird.ModelRow<T>? {
        return try await database.core.performGated { try row(from: $0, columns: columns) }
    }

    /// Synchronous version of ``row(from:columns:)`` for use in database transactions.
    public func row(from core: isolated Blackbird.Database.Core, columns: [T.BlackbirdColumnKeyPath]) throws -> Blackbird.ModelRow<T>? {
        return try T.query(in: core, columns: columns, matching: .literal("rowid = ?", rowid)).first
    }

    /// The text of the given column, with the highlighting prefix and suffix strings, if this column matched the search query.
    /// - Parameter column: The desired column key-path.
    /// - Returns: The source text with matches surrounded by the highlight prefix and suffix strings, or `nil` if highlights were not generated by the search.
    public func highlighted(_ column: T.BlackbirdColumnKeyPath) -> String? {
        if case .generate(_, _) = highlightMode, let text = highlights?.value(keyPath: column)?.stringValue {
            return text
        } else {
            return nil
        }
    }

    /// A snippet of matching text, with the snippet-highlighting prefix and suffix strings, if this column matched the search query.
    /// - Parameter column: The desired column key-path.
    /// - Returns: The snippet of matching text with matches surrounded by the snippet prefix and suffix strings, or `nil` if this column didn't match the query or snippets were not generated by the search.
    public func snippet(_ column: T.BlackbirdColumnKeyPath) -> String? {
        if case let .generate(_, prefix, suffix, _) = snippetMode, let text = snippets?.value(keyPath: column)?.stringValue, text.contains(prefix), text.contains(suffix) {
            return text
        } else {
            return nil
        }
    }
    
    /// The text of the given column, with matches highlighted, if this column matched the search query.
    /// - Parameters:
    ///   - column: The desired column key-path.
    ///   - matchAttributes: The attributes to apply to matching substrings.
    /// - Returns: The source text with matches having the given attributes, or `nil` if highlights were not generated by the search.
    public func highlighted(_ column: T.BlackbirdColumnKeyPath, matchAttributes: AttributeContainer) -> AttributedString? {
        guard case let .generate(prefix, suffix) = highlightMode, let text = highlights?.value(keyPath: column)?.stringValue else { return nil }
        return higlightedText(text, prefix: prefix, suffix: suffix, matchAttributes: matchAttributes)
    }

    /// A snippet of matching text, with matches highlighted, if this column matched the search query.
    /// - Parameters:
    ///   - column: The desired column key-path.
    ///   - matchAttributes: The attributes to apply to matching substrings.
    /// - Returns: The snippet of matching text with matches having the given attributes, or `nil` if this column didn't match the query or snippets were not generated by the search.
    public func snippet(_ column: T.BlackbirdColumnKeyPath, matchAttributes: AttributeContainer) -> AttributedString? {
        guard case let .generate(_, prefix, suffix, _) = snippetMode, let text = snippets?.value(keyPath: column)?.stringValue, text.contains(prefix), text.contains(suffix) else { return nil }
        return higlightedText(text, prefix: prefix, suffix: suffix, matchAttributes: matchAttributes)
    }

    private func higlightedText(_ text: String, prefix: String, suffix: String, matchAttributes: AttributeContainer) -> AttributedString {
        var attrString = AttributedString()
        
        let scanner = Scanner(string: text)
        scanner.charactersToBeSkipped = nil
        
        while !scanner.isAtEnd {
            if let before = scanner.scanUpToString(prefix) { attrString.append(AttributedString(before)) }

            if scanner.scanString(prefix) == prefix, let highlighted = scanner.scanUpToString(suffix) {
                attrString.append(AttributedString(highlighted, attributes: matchAttributes))
                _ = scanner.scanString(suffix)
            }
        }
        return attrString
    }
}

/// How a full-text-search query should be escaped or processed.
public enum BlackbirdFullTextQuerySyntaxMode: Sendable {
    /// The query will be passed directly to SQLite, supporting [the complete FTS5 complete syntax](https://sqlite.org/fts5.html#full_text_query_syntax).
    case allowFullQuerySyntax
    
    /// All entered text will be passed to SQLite as literal phrases, without supporting [SQLite FTS operators](https://sqlite.org/fts5.html#full_text_query_syntax).
    case escapeQuerySyntax
    
    /// All entered text will be passed to SQLite as literal phrases, without supporting [SQLite FTS operators](https://sqlite.org/fts5.html#full_text_query_syntax).
    /// The last phrase in a query will be modified to also match any terms that begin with it, so e.g. `"accidental tech pod"` would match `"accidental tech podcast"`.
    case escapeQuerySyntaxAndPrefixMatchLastPhrase
}

fileprivate struct BlackbirdFullTextQuerySyntaxPhrase {
    let phrase: String
    let wasQuoted: Bool
}

/// The outcome of a `continueIncrementalFullTextIndexRebuild` call on a ``BlackbirdModel``.
public enum BlackbirdFullTextIndexRebuildProgress: Sendable, Equatable {
    /// No incremental rebuild is in progress.
    case notInProgress

    /// The rebuild completed during this call. The index is now complete, and normal
    /// index-maintenance triggers are restored.
    case done(rowsIndexed: Int64)

    /// The time budget elapsed with rows still waiting to be indexed; call again later to
    /// continue from where this call left off.
    case timeLimitReached(rowsIndexed: Int64, rowsRemaining: Int64)
}

extension BlackbirdModel {
    public typealias SearchResult = BlackbirdModelSearchResult<Self>

    /// Consolidate the full-text index into an optimized structure for fastest querying.
    ///
    /// This is a heavy operation that may take noticeable time on large indexes.
    public static func optimizeFullTextIndex(in database: Blackbird.Database) async throws {
        try await database.core.performGated { try optimizeFullTextIndex(core: $0) }
    }

    /// Synchronous version of ``optimizeFullTextIndex(in:)``.
    public static func optimizeFullTextIndex(core: isolated Blackbird.Database.Core) throws {
        if Self.fullTextSearchableColumns.isEmpty { return }
    
        let ftsTable = Blackbird.Table.FullTextIndexSchema.ftsTableName(Self.tableName)
        try core.query("INSERT INTO `\(ftsTable)`(`\(ftsTable)`) VALUES ('optimize')")
        try core.query("PRAGMA wal_checkpoint(TRUNCATE)")
        try core.query("PRAGMA incremental_vacuum(16)")
    }
    
    public typealias FullTextIndexRebuildProgress = BlackbirdFullTextIndexRebuildProgress

    /// Begins an incremental rebuild of this model's full-text index.
    ///
    /// The index is emptied and every existing row is queued for re-indexing in batches by
    /// subsequent ``continueIncrementalFullTextIndexRebuild(in:timeLimit:batchSize:)`` calls,
    /// so no single database operation ever re-indexes the whole table the way a full rebuild
    /// does. Rows inserted, changed, or deleted while the rebuild is in progress are reflected
    /// in the index immediately, and the queued state survives closing and reopening the
    /// database, so an interrupted rebuild resumes instead of starting over.
    ///
    /// Searches performed during the rebuild return results only from rows indexed so far.
    ///
    /// Rarely needs to be called directly: schema resolution starts one automatically when a
    /// model with ``BlackbirdModel/fullTextIndexRebuildsIncrementally`` set to `true` requires
    /// an index rebuild. Calling it directly is useful to repair index contents in place (e.g.
    /// after rows were changed while index maintenance was broken), since it works regardless
    /// of that setting. Calling it while a rebuild is already in progress restarts the rebuild.
    public static func beginIncrementalFullTextIndexRebuild(in database: Blackbird.Database) async throws {
        guard let fullTextIndex = SchemaGenerator.shared.table(for: Self.self).fullTextIndex else { return }
        try await database.core.performGated { try fullTextIndex.startIncrementalRebuild(core: $0) }
    }

    /// Continues an in-progress incremental rebuild of this model's full-text index, indexing
    /// batches of rows until it finishes or the given time limit elapses.
    ///
    /// Each batch runs as its own short transaction, so other database work interleaves between
    /// batches instead of blocking for the rebuild's duration. Call periodically — e.g. from a
    /// background task — until it returns ``FullTextIndexRebuildProgress/done(rowsIndexed:)``.
    ///
    /// - Parameters:
    ///   - database: The ``Blackbird/Database`` to operate on.
    ///   - timeLimit: The maximum time to spend indexing, in seconds.
    ///   - batchSize: The number of rows to index per transaction.
    /// - Returns: A ``FullTextIndexRebuildProgress`` describing what was done.
    @discardableResult
    public static func continueIncrementalFullTextIndexRebuild(in database: Blackbird.Database, timeLimit: TimeInterval = 2.0, batchSize: Int = 500) async throws -> FullTextIndexRebuildProgress {
        guard let fullTextIndex = SchemaGenerator.shared.table(for: Self.self).fullTextIndex else { return .notInProgress }

        let startNanoseconds = DispatchTime.now().uptimeNanoseconds
        var rowsIndexed: Int64 = 0
        var wasInProgress = false

        while true {
            let batchResult: (rowsIndexed: Int64, rowsRemaining: Int64)? = try await database.core.performGated { core in
                guard try fullTextIndex.incrementalRebuildInProgress(core: core) else { return nil }
                return try fullTextIndex.performIncrementalRebuildBatch(core: core, batchSize: batchSize)
            }

            guard let batchResult else { return wasInProgress ? .done(rowsIndexed: rowsIndexed) : .notInProgress }
            wasInProgress = true
            rowsIndexed += batchResult.rowsIndexed

            if batchResult.rowsRemaining <= 0 {
                try await database.core.performGated { try fullTextIndex.finishIncrementalRebuild(core: $0) }
                return .done(rowsIndexed: rowsIndexed)
            }

            let elapsed = Double(DispatchTime.now().uptimeNanoseconds - startNanoseconds) / 1_000_000_000
            if elapsed > timeLimit { return .timeLimitReached(rowsIndexed: rowsIndexed, rowsRemaining: batchResult.rowsRemaining) }
        }
    }

    /// Search this model's full-text index.
    /// - Parameters:
    ///   - database: The database to query.
    ///   - matching: The expression to match. For instance, `.match("tech")`
    ///   - limit: Return at most this many results. If unspecified or `nil`, all matching results are returned.
    ///   - options: Options for this search.
    /// - Returns: The matching set of ``BlackbirdModelSearchResult`` objects.
    ///
    /// This requires that this model has at least one total column, and all columns referenced in the `matching` argument, specified in ``BlackbirdModel/fullTextSearchableColumns``.
    public static func fullTextSearch(from database: Blackbird.Database, matching: BlackbirdModelColumnExpression<Self>, limit: Int? = nil, options: BlackbirdModelSearchOptions<Self> = .init()) async throws -> [Self.SearchResult] {
        try await database.core.performGated { try fullTextSearch(from: $0, matching: matching, limit: limit, options: options) }
    }

    /// Synchronous version of ``fullTextSearch(from:matching:limit:options:)``.
    public static func fullTextSearch(from core: isolated Blackbird.Database.Core, matching: BlackbirdModelColumnExpression<Self>, limit: Int? = nil, options: BlackbirdModelSearchOptions<Self> = .init()) throws -> [Self.SearchResult] {
        let decoded = DecodedStructuredFTSQuery<Self>(matching: matching, options: options, limit: limit)
        return try decoded.query(in: core, scoreMultiplier: options.scoreMultiple)
    }

    internal static func fullTextQueryEscape(_ query: String, mode: BlackbirdFullTextQuerySyntaxMode) -> String {
        if mode == .allowFullQuerySyntax { return query }

        let whitespaceCharacters = CharacterSet.whitespacesAndNewlines
        let phraseDelimters = CharacterSet(charactersIn:
            "\"'“”‘’`«‹»›}\u{201E}\u{201A}\u{201C}\u{201F}\u{2018}\u{201B}\u{201D}\u{2019}\u{275B}\u{275C}\u{275F}\u{275D}\u{275E}\u{276E}\u{276F}\u{2E42}\u{301D}\u{301E}\u{301F}\u{FF02}"
        )

        var isQuotedPhrase = false
        var phrases: [BlackbirdFullTextQuerySyntaxPhrase] = []
        var currentPhraseWords: [String] = []
        var currentWord = ""

        let endWord = {
            if !currentWord.isEmpty {
                currentPhraseWords.append(currentWord)
                currentWord = ""
            }
        }

        let endPhrase = { (wasQuoted: Bool) in
            endWord()
            let phrase = currentPhraseWords.joined(separator: " ")
            if !phrase.isEmpty {
                phrases.append(BlackbirdFullTextQuerySyntaxPhrase(phrase: phrase, wasQuoted: wasQuoted))
            }
            currentPhraseWords.removeAll()
        }

        for scalar in query.unicodeScalars {
            if phraseDelimters.contains(scalar) {
                if isQuotedPhrase {
                    isQuotedPhrase = false
                    endPhrase(true)
                } else if currentWord.isEmpty {
                    endPhrase(false)
                    isQuotedPhrase = true
                } else {
                    currentWord.unicodeScalars.append(scalar)
                }
            } else if whitespaceCharacters.contains(scalar) {
                if isQuotedPhrase { endWord() } else { endPhrase(false) }
            } else {
                currentWord.unicodeScalars.append(scalar)
            }
        }
        endPhrase(isQuotedPhrase)

        let escapedQuery = phrases.map { "\"\($0.phrase.replacingOccurrences(of: "\"", with: "\"\""))\"" }.joined(separator: " ")
        if mode == .escapeQuerySyntaxAndPrefixMatchLastPhrase, let last = phrases.last, !last.wasQuoted {
            return "\(escapedQuery) *"
        } else {
            return escapedQuery
        }
    }
}

fileprivate struct DecodedStructuredFTSQuery<T: BlackbirdModel>: Sendable {
    let query: String
    let arguments: [Sendable]
    let table: Blackbird.Table
    let cacheKey: [Blackbird.Value]?
    
    let highlightColumnPrefix = "FTSHighlight"
    let snippetColumnPrefix = "FTSSnippet"
    let scoreColumnName = "FTS+Score"
    let fieldNames: [String]
    let options: BlackbirdModelSearchOptions<T>
    
    init(matching: BlackbirdModelColumnExpression<T>, options: BlackbirdModelSearchOptions<T>, limit: Int?) {
        self.options = options
        table = SchemaGenerator.shared.table(for: T.self)

        guard let fullTextIndex = table.fullTextIndex else {
            fatalError("[Blackbird] \(String(describing: T.self)) does not define any fullTextSearchableColumns.")
        }

        for orderBy in options.orderBy {
            switch orderBy.direction {
                case .ascending(column: let column):
                    guard T.fullTextSearchableColumns[column] != nil else {
                        let keyPathName = table.keyPathToColumnName(keyPath: column)
                        fatalError("Column \\.$\(keyPathName) in full-text-search order-by clause is not included in `\(table.name).fullTextSearchableColumns`. Consider adding it as .filterOnly.")
                    }

                case .descending(column: let column):
                    guard T.fullTextSearchableColumns[column] != nil else {
                        let keyPathName = table.keyPathToColumnName(keyPath: column)
                        fatalError("Column \\.$\(keyPathName) in full-text-search order-by clause is not included in `\(table.name).fullTextSearchableColumns`. Consider adding it as .filterOnly.")
                    }

                case .random:
                    break
            }
        }
        
        let ftsTableName = Blackbird.Table.FullTextIndexSchema.ftsTableName(T.tableName)
        var clauses: [String] = []
        var arguments: [Blackbird.Value] = []
        
        fieldNames = fullTextIndex.sortedFieldNames
        let fieldWeights = fieldNames.map { fullTextIndex.fields[$0]?.weight ?? 0.0 }

        var bm25expr = "(-1 * bm25(`\(ftsTableName)`,\(fieldWeights.map { "\($0)" }.joined(separator: ","))))"
        if let scoreMultipleColumn = options.scoreMultipleColumn {
            bm25expr += " * `\(table.keyPathToFTSColumnName(keyPath: scoreMultipleColumn))`"
        }

        var columnsToSelect: [String] = [
            "rowid",
            "\(bm25expr) AS `\(scoreColumnName)`"
        ]

        if case let .generate(prefix, suffix) = options.highlights {
            var idx = 0
            for _ in fieldNames {
                columnsToSelect.append("highlight(`\(ftsTableName)`, \(idx), ?, ?) AS `\(highlightColumnPrefix)+\(idx)`")
                arguments.append(.text(prefix))
                arguments.append(.text(suffix))
                idx += 1
            }
        }

        if case let .generate(contextWords, prefix, suffix, ellipsis) = options.snippets {
            var idx = 0
            for _ in fieldNames {
                columnsToSelect.append("snippet(`\(ftsTableName)`, \(idx), ?, ?, ?, \(contextWords)) AS `\(snippetColumnPrefix)+\(idx)`")
                arguments.append(.text(prefix))
                arguments.append(.text(suffix))
                arguments.append(.text(ellipsis))
                idx += 1
            }
        }

        let selectClause = "SELECT \(columnsToSelect.joined(separator: ","))"

        let (whereClause, whereArguments) = matching.compile(table: table, queryingFullTextIndex: true)
        if let whereClause { clauses.append("WHERE \(whereClause)") }
        arguments.append(contentsOf: whereArguments)
        
        if !options.orderBy.isEmpty {
            let table = table
            let orderBy = options.orderBy
            clauses.append("ORDER BY \(orderBy.map { $0.orderByClause(table: table) }.joined(separator: ",")) ")
        } else {
            clauses.append("ORDER BY `\(scoreColumnName)` DESC")
        }

        if let limit { clauses.append("LIMIT \(limit)") }

        query = "\(selectClause) FROM `\(ftsTableName)` \(clauses.joined(separator: " "))"
        self.arguments = arguments
        
        var cacheKey = [Blackbird.Value.text(query)]
        cacheKey.append(contentsOf: arguments)
        self.cacheKey = cacheKey
    }
    
    func query(in core: isolated Blackbird.Database.Core, scoreMultiplier: Double) throws -> [BlackbirdModelSearchResult<T>] {
        let ftsRows = try T.query(in: core, query, arguments: arguments)

        var preloadedInstancesByRowID: [Blackbird.Value: T] = [:]
        if options.preloadInstances, !ftsRows.isEmpty {
            let database = try core.database()
            let rowids = ftsRows.compactMap { $0["rowid"] }
            var index = 0
            while index < rowids.count {
                let chunk = Array(rowids[index ..< min(index + database.maxQueryVariableCount, rowids.count)])
                index += chunk.count
                let placeholders = Array(repeating: "?", count: chunk.count).joined(separator: ",")
                // rowid is aliased explicitly: for tables with an INTEGER primary key, a bare
                // `SELECT rowid` returns the column under its alias's name (e.g. `id`) instead
                for row in try T.query(in: core, "SELECT rowid AS `_preload_rowid_`, * FROM $T WHERE rowid IN (\(placeholders))", arguments: chunk) {
                    guard let rowid = row["_preload_rowid_"] else { continue }
                    let instance = try T(from: BlackbirdSQLiteDecoder(database: database, row: row.row))
                    instance._saveCachedInstance(for: database)
                    preloadedInstancesByRowID[rowid] = instance
                }
            }
        }

        var results: [BlackbirdModelSearchResult<T>] = []
        for row in ftsRows {
            if let result = try result(ftsRow: row, scoreMultiplier: scoreMultiplier, preloadedInstancesByRowID: preloadedInstancesByRowID) {
                results.append(result)
            }
        }
        return results
    }

    private func result(ftsRow: Blackbird.ModelRow<T>, scoreMultiplier: Double, preloadedInstancesByRowID: [Blackbird.Value: T]) throws -> BlackbirdModelSearchResult<T>? {
        guard let rowid = ftsRow["rowid"], let score = ftsRow[scoreColumnName] else {
            fatalError("Unexpected result row format from FTS query on \(String(describing: T.self)) (missing rowid or score)")
        }
        
        var idx = 0
        var highlightRow: [String: Blackbird.Value] = [:]
        var snippetRow: [String: Blackbird.Value] = [:]
        for fieldName in fieldNames {
            if let highlight = ftsRow["\(highlightColumnPrefix)+\(idx)"] {
                highlightRow[fieldName] = highlight
            }

            if let snippet = ftsRow["\(snippetColumnPrefix)+\(idx)"] {
                snippetRow[fieldName] = snippet
            }
            idx += 1
        }
        
        let preloadedInstance = options.preloadInstances ? preloadedInstancesByRowID[rowid] : nil
        return BlackbirdModelSearchResult<T>(highlights: .init(highlightRow, table: table), highlightMode: options.highlights, snippets: .init(snippetRow, table: table), snippetMode: options.snippets, rowid: rowid, score: (score.doubleValue ?? 0) * scoreMultiplier, preloadedInstance: preloadedInstance)
    }
}

extension Blackbird.Table {
    internal struct FullTextIndexSchema: Equatable, Hashable, Sendable {
        static let defaultTokenizer = "porter unicode61 remove_diacritics 2"

        public static func == (lhs: Self, rhs: Self) -> Bool { return lhs.contentTableName == rhs.contentTableName && lhs.fields == rhs.fields && lhs.tokenizer == rhs.tokenizer }
        public func hash(into hasher: inout Hasher) {
            hasher.combine(contentTableName)
            hasher.combine(fields)
            hasher.combine(tokenizer)
        }
    
        internal let contentTableName: String
        internal let fields: [String: BlackbirdModelFullTextSearchableColumn]
        internal let tokenizer: String
        
        internal var sortedFieldNames: [String] { fields.keys.sorted { $0 < $1 } }
        
        internal static func ftsTableName(_ contentTableName: String) -> String { "\(contentTableName)+FTS" }
        internal static func insertTriggerName(_ contentTableName: String) -> String { "\(contentTableName)+FTSInsert" }
        internal static func updateTriggerName(_ contentTableName: String) -> String { "\(contentTableName)+FTSUpdate" }
        internal static func deleteTriggerName(_ contentTableName: String) -> String { "\(contentTableName)+FTSDelete" }
        internal static func updateTriggerPendingName(_ contentTableName: String) -> String { "\(contentTableName)+FTSUpdatePending" }
        internal static func deleteTriggerPendingName(_ contentTableName: String) -> String { "\(contentTableName)+FTSDeletePending" }
        internal static func rebuildPendingTableName(_ contentTableName: String) -> String { "\(contentTableName)+FTSRebuildPending" }

        internal static func allTriggerNames(_ contentTableName: String) -> [String] {
            [
                insertTriggerName(contentTableName),
                updateTriggerName(contentTableName),
                updateTriggerPendingName(contentTableName),
                deleteTriggerName(contentTableName),
                deleteTriggerPendingName(contentTableName),
            ]
        }
        
        internal var ftsTableDefinition: String {
            let fieldDefinitions = sortedFieldNames.map {
                fields[$0]!.indexed ? "`\($0)`" : "`\($0)` UNINDEXED"
            }
            
            return "CREATE VIRTUAL TABLE `\(Self.ftsTableName(contentTableName))` USING fts5(\(fieldDefinitions.joined(separator: ",")),content=`\(contentTableName)`,tokenize='\(tokenizer)')"
        }
        
        internal static func ftsTableExists(core: isolated Blackbird.Database.Core, contentTableName: String) throws -> Bool {
            return !(try core.query("SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?", arguments: [ftsTableName(contentTableName)])).isEmpty
        }
        
        private struct TriggerFieldLists {
            let fieldNames: String
            let oldFieldNames: String
            let newFieldNames: String
            let updateOfFieldNames: String
        }

        private func triggerFieldLists(core: isolated Blackbird.Database.Core) throws -> TriggerFieldLists {
            let rawFieldNames = sortedFieldNames

            let primaryKeyNames = try core.query("PRAGMA table_info('\(contentTableName)')")
                .filter { ($0["pk"]?.intValue ?? 0) > 0 }
                .sorted { ($0["pk"]?.intValue ?? 0) < ($1["pk"]?.intValue ?? 0) }
                .compactMap { $0["name"]?.stringValue }
            var updateOfRawFieldNames = rawFieldNames
            for primaryKeyName in primaryKeyNames where !updateOfRawFieldNames.contains(primaryKeyName) {
                updateOfRawFieldNames.append(primaryKeyName)
            }

            return TriggerFieldLists(
                fieldNames: rawFieldNames.map { "`\($0)`" }.joined(separator: ","),
                oldFieldNames: rawFieldNames.map { "OLD.`\($0)`" }.joined(separator: ","),
                newFieldNames: rawFieldNames.map { "NEW.`\($0)`" }.joined(separator: ","),
                updateOfFieldNames: updateOfRawFieldNames.map { "`\($0)`" }.joined(separator: ",")
            )
        }

        // The expected CREATE TRIGGER statements, keyed by trigger name. Compared verbatim
        // against sqlite_master by resolutionState(core:), so any change here retroactively
        // upgrades existing databases' triggers at their next schema resolution — without
        // rebuilding the index, since trigger definitions only affect future maintenance.
        private func triggerDefinitions(core: isolated Blackbird.Database.Core) throws -> [String: String] {
            let insertTriggerName = Self.insertTriggerName(contentTableName)
            let updateTriggerName = Self.updateTriggerName(contentTableName)
            let deleteTriggerName = Self.deleteTriggerName(contentTableName)
            let ftsTableName = Self.ftsTableName(contentTableName)
            let f = try triggerFieldLists(core: core)

            return [
                insertTriggerName:
                    """
                    CREATE TRIGGER `\(insertTriggerName)` AFTER INSERT ON `\(contentTableName)` BEGIN
                        INSERT INTO `\(ftsTableName)`(rowid,\(f.fieldNames)) VALUES (NEW.rowid,\(f.newFieldNames));
                    END
                    """,
                updateTriggerName:
                    """
                    CREATE TRIGGER `\(updateTriggerName)` AFTER UPDATE OF \(f.updateOfFieldNames) ON `\(contentTableName)` BEGIN
                        INSERT INTO `\(ftsTableName)`(`\(ftsTableName)`,rowid,\(f.fieldNames)) VALUES ('delete',OLD.rowid,\(f.oldFieldNames));
                        INSERT INTO `\(ftsTableName)`(rowid,\(f.fieldNames)) VALUES (NEW.rowid,\(f.newFieldNames));
                    END
                    """,
                deleteTriggerName:
                    """
                    CREATE TRIGGER `\(deleteTriggerName)` AFTER DELETE ON `\(contentTableName)` BEGIN
                        INSERT INTO `\(ftsTableName)`(`\(ftsTableName)`,rowid,\(f.fieldNames)) VALUES ('delete',OLD.rowid,\(f.oldFieldNames));
                    END
                    """,
            ]
        }

        // The trigger set active during an incremental rebuild. Rows still listed in the pending
        // table aren't in the index yet, so they must not issue FTS 'delete' commands — deleting
        // entries that were never inserted corrupts external-content FTS5 indexes. Instead,
        // changed pending rows are indexed immediately and removed from the pending queue
        // (or just dequeued, when deleted), so the index stays exact for every row that has
        // either been backfilled or touched since the rebuild began.
        //
        // The pending-row condition lives INSIDE each trigger body (`INSERT … SELECT … WHERE NOT
        // EXISTS`), deliberately not as a pair of triggers with complementary WHEN clauses:
        // SQLite evaluates each trigger's WHEN immediately before firing it, so a first trigger
        // that dequeues the row would flip a second trigger's `WHEN NOT EXISTS` to true and fire
        // both, corrupting the index — dependent on trigger-creation order.
        private func rebuildTriggerDefinitions(core: isolated Blackbird.Database.Core) throws -> [String: String] {
            let insertTriggerName = Self.insertTriggerName(contentTableName)
            let updateTriggerName = Self.updateTriggerName(contentTableName)
            let deleteTriggerName = Self.deleteTriggerName(contentTableName)
            let ftsTableName = Self.ftsTableName(contentTableName)
            let pendingTableName = Self.rebuildPendingTableName(contentTableName)
            let f = try triggerFieldLists(core: core)

            return [
                insertTriggerName:
                    """
                    CREATE TRIGGER `\(insertTriggerName)` AFTER INSERT ON `\(contentTableName)` BEGIN
                        INSERT INTO `\(ftsTableName)`(rowid,\(f.fieldNames)) VALUES (NEW.rowid,\(f.newFieldNames));
                        DELETE FROM `\(pendingTableName)` WHERE rowid = NEW.rowid;
                    END
                    """,
                updateTriggerName:
                    """
                    CREATE TRIGGER `\(updateTriggerName)` AFTER UPDATE OF \(f.updateOfFieldNames) ON `\(contentTableName)` BEGIN
                        INSERT INTO `\(ftsTableName)`(`\(ftsTableName)`,rowid,\(f.fieldNames)) SELECT 'delete',OLD.rowid,\(f.oldFieldNames) WHERE NOT EXISTS (SELECT 1 FROM `\(pendingTableName)` WHERE rowid = OLD.rowid);
                        INSERT INTO `\(ftsTableName)`(rowid,\(f.fieldNames)) VALUES (NEW.rowid,\(f.newFieldNames));
                        DELETE FROM `\(pendingTableName)` WHERE rowid = OLD.rowid;
                    END
                    """,
                deleteTriggerName:
                    """
                    CREATE TRIGGER `\(deleteTriggerName)` AFTER DELETE ON `\(contentTableName)` BEGIN
                        INSERT INTO `\(ftsTableName)`(`\(ftsTableName)`,rowid,\(f.fieldNames)) SELECT 'delete',OLD.rowid,\(f.oldFieldNames) WHERE NOT EXISTS (SELECT 1 FROM `\(pendingTableName)` WHERE rowid = OLD.rowid);
                        DELETE FROM `\(pendingTableName)` WHERE rowid = OLD.rowid;
                    END
                    """,
            ]
        }

        internal func dropAllTriggers(core: isolated Blackbird.Database.Core) throws {
            for triggerName in Self.allTriggerNames(contentTableName) {
                try core.query("DROP TRIGGER IF EXISTS `\(triggerName)`")
            }
        }

        internal func recreateTriggers(core: isolated Blackbird.Database.Core) throws {
            try dropAllTriggers(core: core)

            for (_, triggerSQL) in try triggerDefinitions(core: core) {
                try core.query(triggerSQL)
            }
        }

        internal func rebuild(core: isolated Blackbird.Database.Core) throws {
            let ftsTableName = Self.ftsTableName(contentTableName)
            try core.query("DROP TABLE IF EXISTS `\(ftsTableName)`")
            try core.query("DROP TABLE IF EXISTS `\(Self.rebuildPendingTableName(contentTableName))`") // cancels any incremental rebuild in progress
            try core.query(ftsTableDefinition)
            try recreateTriggers(core: core)
            try core.query("INSERT INTO `\(ftsTableName)`(`\(ftsTableName)`) VALUES('rebuild')")
        }

        internal enum ResolutionState {
            /// The FTS table and triggers match the current definitions.
            case upToDate

            /// The FTS table matches, but the triggers are outdated (or missing, or stale
            /// rebuild-mode leftovers). They can be recreated in place: trigger definitions only
            /// affect future index maintenance, so the indexed content remains valid and no
            /// rebuild is needed.
            case triggersOutdated

            /// The FTS table itself is missing or its definition has changed: the index must be
            /// rebuilt from scratch.
            case rebuildRequired

            /// An incremental rebuild started earlier is intact and still in progress.
            case rebuildInProgress
        }

        internal func resolutionState(core: isolated Blackbird.Database.Core) throws -> ResolutionState {
            let ftsTableName = Self.ftsTableName(contentTableName)
            let createdWithSQL = try core.query("SELECT sql FROM sqlite_master WHERE name = '\(ftsTableName)'").first?["sql"]?.stringValue ?? ""
            if createdWithSQL != ftsTableDefinition { return .rebuildRequired }

            // Compared by SQL, not just name, so databases with outdated trigger
            // definitions (e.g. from before primary-key columns were added to the
            // update trigger) get their triggers upgraded at the next schema resolution.
            var existingTriggerSQL: [String: String] = [:]
            for row in try core.query("SELECT name, sql FROM sqlite_master WHERE type = 'trigger'") {
                guard let name = row["name"]?.stringValue, let sql = row["sql"]?.stringValue else { continue }
                existingTriggerSQL[name] = sql
            }

            if try incrementalRebuildInProgress(core: core) {
                // Valid only if the entire rebuild-mode trigger set matches; anything else is a
                // stale or unknown rebuild state, and the rebuild must restart.
                for (triggerName, expectedSQL) in try rebuildTriggerDefinitions(core: core) {
                    if existingTriggerSQL[triggerName] != expectedSQL { return .rebuildRequired }
                }
                return .rebuildInProgress
            }

            // Stale rebuild-mode triggers without their pending table would fail at runtime
            if existingTriggerSQL[Self.updateTriggerPendingName(contentTableName)] != nil || existingTriggerSQL[Self.deleteTriggerPendingName(contentTableName)] != nil {
                return .triggersOutdated
            }

            for (triggerName, expectedSQL) in try triggerDefinitions(core: core) {
                if existingTriggerSQL[triggerName] != expectedSQL { return .triggersOutdated }
            }

            return .upToDate
        }

        // MARK: - Incremental rebuilds

        internal func incrementalRebuildInProgress(core: isolated Blackbird.Database.Core) throws -> Bool {
            return !(try core.query("SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?", arguments: [Self.rebuildPendingTableName(contentTableName)])).isEmpty
        }

        // Begins an incremental rebuild: the index is emptied, every existing rowid is queued in
        // the pending table, and rebuild-mode triggers keep the index exact for rows that change
        // before their backfill batch arrives. Searches during the rebuild return results only
        // from rows indexed so far.
        internal func startIncrementalRebuild(core: isolated Blackbird.Database.Core) throws {
            let ftsTableName = Self.ftsTableName(contentTableName)
            let pendingTableName = Self.rebuildPendingTableName(contentTableName)

            try core.transaction { core in
                try core.query("DROP TABLE IF EXISTS `\(ftsTableName)`")
                try core.query("DROP TABLE IF EXISTS `\(pendingTableName)`")
                try core.query(ftsTableDefinition)
                try core.query("CREATE TABLE `\(pendingTableName)` (rowid INTEGER PRIMARY KEY)")
                try core.query("INSERT INTO `\(pendingTableName)` SELECT _rowid_ FROM `\(contentTableName)`")
                try dropAllTriggers(core: core)
                for (_, triggerSQL) in try rebuildTriggerDefinitions(core: core) { try core.query(triggerSQL) }
            }
        }

        // Indexes one batch of pending rows. Returns the number of pending entries processed and
        // the number remaining; the caller finishes the rebuild when none remain.
        internal func performIncrementalRebuildBatch(core: isolated Blackbird.Database.Core, batchSize: Int) throws -> (rowsIndexed: Int64, rowsRemaining: Int64) {
            let ftsTableName = Self.ftsTableName(contentTableName)
            let pendingTableName = Self.rebuildPendingTableName(contentTableName)
            let fieldNames = sortedFieldNames.map { "`\($0)`" }.joined(separator: ",")

            return try core.transaction { core in
                let batchRowids = try core.query("SELECT rowid FROM `\(pendingTableName)` ORDER BY rowid LIMIT \(batchSize)").compactMap { $0["rowid"]?.int64Value }
                if batchRowids.isEmpty { return (0, 0) }

                // The same explicit rowid list for both statements: the indexed set and the
                // dequeued set must be identical, or rows could be indexed twice or never.
                let rowidList = batchRowids.map { String($0) }.joined(separator: ",")
                try core.query("INSERT INTO `\(ftsTableName)`(rowid,\(fieldNames)) SELECT _rowid_,\(fieldNames) FROM `\(contentTableName)` WHERE _rowid_ IN (\(rowidList))")
                try core.query("DELETE FROM `\(pendingTableName)` WHERE rowid IN (\(rowidList))")

                let rowsRemaining = try core.query("SELECT COUNT(*) AS c FROM `\(pendingTableName)`").first?["c"]?.int64Value ?? 0
                return (Int64(batchRowids.count), rowsRemaining)
            }
        }

        internal func finishIncrementalRebuild(core: isolated Blackbird.Database.Core) throws {
            try core.transaction { core in
                try core.query("DROP TABLE IF EXISTS `\(Self.rebuildPendingTableName(contentTableName))`")
                try recreateTriggers(core: core)
            }
        }
        
        internal init(contentTableName: String, fields: [String: BlackbirdModelFullTextSearchableColumn], tokenizer: String? = nil) {
            guard 0 != sqlite3_compileoption_used("ENABLE_FTS5") else { fatalError("[Blackbird] SQLite was compiled without FTS5 support, but a full-text index is specified on table `\(contentTableName)`") }
            guard !fields.isEmpty else { fatalError("No columns specified") }
            self.contentTableName = contentTableName
            self.fields = fields
            self.tokenizer = tokenizer ?? Self.defaultTokenizer
        }
    }
}
