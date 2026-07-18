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
//  BlackbirdVacuum.swift
//  Created by Marco Arment on 7/18/26.
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

extension Blackbird.Database {
    /// The outcome of an ``incrementalVacuum(timeLimit:allowConversionUpToBytes:)`` call.
    public enum IncrementalVacuumResult: Sendable {
        /// The freelist was fully reclaimed within the time budget.
        case done(pagesReclaimed: Int64)

        /// The time budget elapsed with free pages remaining; call again later to continue.
        case timeLimitReached(pagesReclaimed: Int64)

        /// The database was not in incremental auto-vacuum mode and was converted with a
        /// one-time full `VACUUM`.
        case converted

        /// The database is not in incremental auto-vacuum mode, and its total size exceeded
        /// `allowConversionUpToBytes`, so nothing was done. Call again with a larger allowance
        /// from a context where a long-running full `VACUUM` is acceptable.
        case needsConversion
    }

    /// Reclaims free space in small batches, never running longer than the given time limit.
    ///
    /// A full `VACUUM` rewrites the entire database in one long statement, blocking all other
    /// access to the database for its duration. This method instead uses SQLite's
    /// [incremental auto-vacuum](https://www.sqlite.org/pragma.html#pragma_incremental_vacuum)
    /// to reclaim free pages in ~1 MB batches, checking the elapsed time between batches and
    /// stopping once `timeLimit` has passed. Reclamation resumes from where it left off on the
    /// next call.
    ///
    /// Incremental auto-vacuum reclaims unused pages but does not defragment like a full `VACUUM`.
    ///
    /// A database not already in incremental auto-vacuum mode can only enter it via a one-time
    /// full `VACUUM`, which SQLite cannot split into parts. This method performs that conversion
    /// automatically, but only when the database's total size is within `allowConversionUpToBytes`,
    /// so callers can restrict the potentially-long conversion to contexts where it's safe:
    /// pass `.max` for newly-created or small databases, or from long-running background-processing
    /// contexts; pass `0` (or omit) to never convert here and get ``IncrementalVacuumResult/needsConversion``.
    ///
    /// - Parameters:
    ///   - timeLimit: The maximum time to spend reclaiming pages, in seconds.
    ///   - allowConversionUpToBytes: The maximum total database size for which a one-time
    ///       full-`VACUUM` conversion to incremental auto-vacuum mode may be performed. Defaults to `0`.
    /// - Returns: An ``IncrementalVacuumResult`` describing what was done.
    @discardableResult
    public func incrementalVacuum(timeLimit: TimeInterval, allowConversionUpToBytes: Int64 = 0) async throws -> IncrementalVacuumResult {
        let autoVacuumMode = try await query("PRAGMA auto_vacuum").first?["auto_vacuum"]?.int64Value ?? 0
        if autoVacuumMode != 2 /* INCREMENTAL */ {
            let pageSize = try await query("PRAGMA page_size").first?["page_size"]?.int64Value ?? 4096
            let pageCount = try await query("PRAGMA page_count").first?["page_count"]?.int64Value ?? 0
            guard pageCount * pageSize <= allowConversionUpToBytes else { return .needsConversion }

            try await execute("PRAGMA auto_vacuum = INCREMENTAL")
            try await execute("VACUUM")
            return .converted
        }

        let startNanoseconds = DispatchTime.now().uptimeNanoseconds
        let batchPages: Int64 = 256 // ~1 MB per batch at 4 KB pages: each batch takes milliseconds
        var pagesReclaimed: Int64 = 0

        while true {
            let freePagesBefore = try await query("PRAGMA freelist_count").first?["freelist_count"]?.int64Value ?? 0
            if freePagesBefore == 0 { return .done(pagesReclaimed: pagesReclaimed) }

            _ = try await query("PRAGMA incremental_vacuum(\(batchPages))")

            let freePagesAfter = try await query("PRAGMA freelist_count").first?["freelist_count"]?.int64Value ?? 0
            pagesReclaimed += max(0, freePagesBefore - freePagesAfter)

            if freePagesAfter >= freePagesBefore { return .done(pagesReclaimed: pagesReclaimed) } // no progress; don't spin

            let elapsed = Double(DispatchTime.now().uptimeNanoseconds - startNanoseconds) / 1_000_000_000
            if elapsed > timeLimit { return .timeLimitReached(pagesReclaimed: pagesReclaimed) }
        }
    }
}
