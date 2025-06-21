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
//  BlackbirdCodingKey.swift
//  Created by Marco Arment on 4/26/23.
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

/// For BlackbirdModel to work with custom `CodingKeys`, their `CodingKeys` enum must be declared as: `enum CodingKeys: String, BlackbirdCodingKey`.
public protocol BlackbirdCodingKey: RawRepresentable, CodingKey, CaseIterable where RawValue == String {
}

extension BlackbirdCodingKey {
    internal static var allLabeledCases: [String: String] {
        var columnsToKeys: [String: String] = [:]
        for c in allCases {
            guard let value = Self(rawValue: c.stringValue) else { fatalError("Cannot parse CodingKey from string: \"\(c.stringValue)\"") }
            guard let label = _getEnumCaseName(for: value) else { fatalError("Cannot get CodingKey label from string: \"\(c.stringValue)\"") }
            columnsToKeys[label] = c.stringValue
        }
        return columnsToKeys
    }

    // This unfortunate hack is needed to get the name of a CodingKeys enum, e.g. in this example:
    //
    // struct CodingKeys: CodingKey {
    //     case id = "customID"
    // }
    //
    // ...getting the string "id" when supplied with the rawValue of "customID".
    //
    // The synthesis of CodingKeys breaks the normal methods of getting enum names, such as String(describing:).
    //
    // So this hack, based on compiler internals that could break in the future, comes from:
    //  https://forums.swift.org/t/getting-the-name-of-a-swift-enum-value/35654/18
    //
    // If it ever breaks, and no other method arrives to get those names, Blackbird can't support custom CodingKeys.
    //
    @_silgen_name("swift_EnumCaseName") private static func _getEnumCaseNameInternal<T>(_ value: T) -> UnsafePointer<CChar>?
    fileprivate static func _getEnumCaseName<T>(for value: T) -> String? {
        guard let stringPtr = _getEnumCaseNameInternal(value) else { return nil }
        return String(validatingCString: stringPtr)
    }
}

