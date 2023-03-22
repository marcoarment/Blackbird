//
//  BlackbirdPerformanceLogger.swift
//  Created for Marco Arment on 12/03/22.
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
import OSLog

/// A logger that emits signposts and log events to the system logging stream.
///
/// ``PerformanceLogger`` creates Logger and OSSignposter instances with the subsystem and category provided.
///
/// The data provided by this logger is best examined in Instruments. To use Instruments to profile tests in Xcode select the
/// tests tab (Command-6), right click on the test (or group of tests), and pick Profile from the popup menu.
/// When Instruments starts pick the Logging profiling template then the Record button to start the profiling session.
/// The `os_log` and `os_signpost` rows will fill up with data captured from the test being profile. You can expand those
/// to pick the specific `subsystem` and `category` the `PerformanceLogger` was configured to use.
/// Above the details pane at the bottom along the left hand side of the window there is a popup control labeled either `List`
/// or `Summary`. By picking `Summary: Intervals` you can see how many of each measured interval took place, the
/// total execution time, the average execution time, etc.
///
/// ## Example
/// ```swift
///
/// let perfLogger = PerformanceLogger(subsytem: Blackbird.loggingSubsystem, category: "Database.Core")
/// // ...
/// let signpostState = perLogger.begin(signpost: .execute, message: "Some explanatory text")
/// // ...
/// // perfLogger.end(state: signpostState)
///
/// }
/// ```
///
extension Blackbird {
    static let loggingSubsystem = "org.marco.blackbird"

    internal struct PerformanceLogger: @unchecked Sendable /* waiting for Sendable compliance in OSLog components */ {
        let log: Logger // The logger object. Exposed so it can be used directly.
        let post: OSSignposter // The signposter object. Exposed so it can be used directly.

        // Enum of all signposts. Signpost IDs will be generate automatically.
        enum Signpost: CaseIterable {
            case openDatabase
            case closeDatabase
            case execute
            case rowsByPreparedFunc
            case cancellableTransaction
        }

        private var spidMap: [Signpost: OSSignpostID]
        
        init(subsystem: String, category: String) {
            log = Logger(subsystem: subsystem, category: category)
            post = OSSignposter(subsystem: subsystem, category: category)
            // Populate our signpost enum to signpost id table.
            spidMap = [:]
            for sp in Signpost.allCases {
                spidMap[sp] = post.makeSignpostID()
            }
        }

        /// Begins a measured time interval
        ///
        /// - Parameters:
        ///   - signpost: A signpost from the Signpost enum.
        ///   - message: An optional message that will be attached to the signpost interval start.
        ///   - name: An optional name for this signpost. The default is the name of the calling function.
        ///             Since intervals may start and end in different functions you may need to spcify your own
        ///             and make sure to use the same name when you call `end()`.
        /// - Returns: An OSSignpostIntervalState instance which is required to end the measured interval.
        ///
        /// ## Examples
        /// ```swift
        /// let signpostState = perfLogger.begin(signpost: .execute, message: "Some Message", name: "MySignpost")
        /// let signpostState = perfLogger.begin(signpost: .execute, message: "Some Message") // name == #function
        /// let signpostState = perfLogger.begin(signpost: .execute)
        /// // ... do work here ...
        /// perfLogger.end(state: signpostState)
        /// ```
        func begin(signpost: Signpost, message: String = "", name: StaticString = #function) -> OSSignpostIntervalState {
            return post.beginInterval(name, id: spidMap[signpost]!, "\(message)")
        }

        /// Ends a measured time interval
        ///
        /// - Parameters:
        ///   - state: The OSSignpostIntervalState returned from calling `begin()`
        ///   - name: The name matching what was passed to `begin`. Defaults to the name of the calling function.
        /// - Returns: None
        ///
        /// ## Examples
        /// ```swift
        /// // ... do work here ...
        /// perfLogger.end(state: signpostState, name: "MySignpost")
        /// perfLogger.end(state: signpostState)
        /// ```
        func end(state: OSSignpostIntervalState, name: StaticString = #function) {
            post.endInterval(name, state)
        }

        // When using the signposter directly this will return the appropriate OSSignpostID
        /// Get an `OSSignpostID` for a given `PerformanceLogger.Signpost`
        ///
        /// - Parameters:
        ///   - for: The signpost to return the underlying OSSignpostID for
        /// - Returns: None
        ///
        /// ## Examples
        /// ```swift
        /// let spid = perLogger.signpostId(for: .execute)
        /// ```
        func signpostId(for sp: Signpost) -> OSSignpostID {
            // Force unwrap because if we don't have a match we're in big trouble and should crash.
            return spidMap[sp]!
        }
    }
}
