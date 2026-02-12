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
    .package(url: "https://github.com/crunchybananas/MCPCore.git", from: "1.0.0"),
    .package(url: "https://github.com/crunchybananas/ast-chunker.git", from: "1.0.0"),
  ],
  targets: [
    .target(
      name: "RAGCore",
      dependencies: [
        .product(name: "CSQLite", package: "MCPCore"),
        .product(name: "ASTChunker", package: "ast-chunker"),
      ]
    ),
    .testTarget(
      name: "RAGCoreTests",
      dependencies: ["RAGCore"]
    ),
  ]
)
