// swift-tools-version: 6.0

import PackageDescription

let package = Package(
  name: "RAGCore",
  platforms: [
    .macOS("26"),
    .iOS("26"),
  ],
  products: [
    .library(
      name: "RAGCore",
      targets: ["RAGCore"]
    ),
  ],
  dependencies: [
    .package(path: "../MCPCore"),
    .package(path: "../ASTChunker"),
  ],
  targets: [
    .target(
      name: "RAGCore",
      dependencies: [
        .product(name: "CSQLite", package: "MCPCore"),
        .product(name: "ASTChunker", package: "ASTChunker"),
      ]
    ),
    .testTarget(
      name: "RAGCoreTests",
      dependencies: ["RAGCore"]
    ),
  ]
)
