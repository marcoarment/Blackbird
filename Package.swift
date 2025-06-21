// swift-tools-version: 6.0

import PackageDescription

let package = Package(
    name: "Blackbird",
    platforms: [
        .macOS(.v12),
        .iOS(.v15),
        .watchOS(.v8),
        .tvOS(.v15),
    ],
    products: [
        .library(
            name: "Blackbird",
            targets: ["Blackbird"]),
    ],
    dependencies: [
    ],
    targets: [
        .target(
            name: "Blackbird",
            dependencies: [],
        ),
        .testTarget(
            name: "BlackbirdTests",
            dependencies: ["Blackbird"]),
    ],
    swiftLanguageModes: [.v6]
)
