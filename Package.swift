// swift-tools-version: 6.0

import PackageDescription

let package = Package(
  name: "RAGCore",
  platforms: [
    .macOS("26"),
    .iOS("26"),
  ],
  products: [
    .library(name: "CSQLite", targets: ["CSQLite"]),
    .library(
      name: "RAGCore",
      targets: ["RAGCore"]
    ),
  ],
  dependencies: [
    .package(url: "https://github.com/crunchybananas/ast-chunker.git", from: "2.1.0"),
  ],
  targets: [
    // Storage belongs to RAGCore. Preserve the implementation and build
    // settings previously supplied by MCPCore 1.6.0.
    .target(
      name: "CSQLite",
      path: "Sources/CSQLite",
      exclude: ["UPSTREAM.md", "LICENSE"],
      sources: ["sqlite3.c"],
      publicHeadersPath: "include",
      cSettings: [
        .define("SQLITE_ENABLE_LOAD_EXTENSION"),
        .define("SQLITE_ENABLE_FTS5"),
        .define("SQLITE_ENABLE_RTREE"),
        .define("SQLITE_ENABLE_JSON1"),
        .define("SQLITE_THREADSAFE", to: "2"),
        .define("SQLITE_DEFAULT_MEMSTATUS", to: "0"),
        .define("SQLITE_DEFAULT_WAL_SYNCHRONOUS", to: "1"),
        .define("SQLITE_LIKE_DOESNT_MATCH_BLOBS"),
        .define("SQLITE_OMIT_DEPRECATED"),
        .define("SQLITE_OMIT_SHARED_CACHE"),
        .define("SQLITE_USE_ALLOCA"),
        .define("SQLITE_ENABLE_COLUMN_METADATA"),
      ],
      linkerSettings: [.linkedLibrary("z")]
    ),
    .target(
      name: "SQLiteVec",
      dependencies: [
        "CSQLite",
      ],
      cSettings: [
        .define("SQLITE_CORE"),
        .define("SQLITE_VEC_STATIC"),
      ]
    ),
    .target(
      name: "RAGCore",
      dependencies: [
        "CSQLite",
        .product(name: "ASTChunker", package: "ast-chunker"),
        "SQLiteVec",
      ]
    ),
    .testTarget(
      name: "RAGCoreTests",
      dependencies: ["RAGCore"]
    ),
  ]
)
