// swift-tools-version: 5.7

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
            swiftSettings: [
              .unsafeFlags(["-Xfrontend", "-warn-concurrency"])
            ]),
        .testTarget(
            name: "BlackbirdTests",
            dependencies: ["Blackbird"]),
    ]
)
