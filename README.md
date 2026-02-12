# RAGCore

A standalone Swift package for **Retrieval-Augmented Generation** (RAG) over local codebases. RAGCore indexes source files into a SQLite database with vector embeddings, enabling semantic code search, dependency graph tracking, and pattern analysis.

## Features

- **Code Indexing** — Scans repositories, chunks source files (AST-aware + line-based), and stores embeddings for semantic search.
- **Vector Search** — Cosine-similarity search with optional [sqlite-vec](https://github.com/asg017/sqlite-vec) acceleration, plus FTS5 text search.
- **Dependency Graph** — Tracks import/require/inherit/conform relationships across files.
- **Pattern Analysis** — Finds duplicates, naming patterns, hotspots, and structural statistics.
- **Lessons Learned** — Records agent mistake→fix patterns with confidence scoring.
- **Protocol-Based Injection** — Embedding providers, chunk analyzers, and memory monitors are injected via protocols, keeping MLX/CoreML/platform concerns out of the package.

## Requirements

- Swift 6.0+
- macOS 26+ / iOS 26+
- Dependencies:
  - `CSQLite` (from [MCPCore](https://github.com/crunchybananas/MCPCore)) — SQLite amalgamation with FTS5, JSON1, RTREE, and extension loading
  - `ASTChunker` — AST-aware source code chunking via SwiftSyntax

## Installation

### Swift Package Manager

```swift
dependencies: [
  .package(url: "https://github.com/crunchybananas/RAGCore.git", from: "0.1.0"),
]
```

Then add `"RAGCore"` to your target's dependencies.

## Architecture

```
RAGCore/
├── Protocols/          # EmbeddingProvider, ChunkAnalyzer, MemoryPressureMonitor
├── Models/             # RAGSearchResult, RAGLesson, RAGChunk, RAGDependency, etc.
├── Chunking/           # HybridChunker, RAGLineChunker, ChunkingHealthTracker
├── Scanning/           # RAGFileScanner (.ragignore, 50+ file extensions)
├── Store/              # RAGStore actor + extensions:
│   ├── RAGStore.swift          # Core actor, lifecycle, helpers
│   ├── +Schema.swift           # SQLite schema v1→v13, migrations
│   ├── +SQLite.swift           # Low-level query helpers, CRUD
│   ├── +Index.swift            # Repository indexing pipeline
│   ├── +Search.swift           # Vector + text search
│   ├── +Analysis.swift         # AI analysis, duplicates, patterns, hotspots
│   ├── +Dependencies.swift     # Import/dependency graph
│   ├── +Lessons.swift          # Learned patterns, query hints
│   └── +Queries.swift          # Orphans, structural queries, similar code
└── Utilities/          # VectorMath, TextSanitizer
```

## Quick Start

```swift
import RAGCore

// 1. Create a store with your embedding provider
let store = RAGStore(
  embeddingProvider: myProvider,       // Conforms to EmbeddingProvider
  chunkAnalyzer: myAnalyzer,           // Optional: conforms to ChunkAnalyzer
  memoryMonitor: myMonitor             // Optional: conforms to MemoryPressureMonitor
)

// 2. Initialize and index a repository
try await store.initialize()
let report = try await store.indexRepository(path: "/path/to/repo")

// 3. Search
let results = try await store.searchVector(query: "authentication flow", repoPath: "/path/to/repo")
for result in results {
  print("\(result.filePath):\(result.startLine) — \(result.constructName ?? "")")
}

// 4. Dependency graph
let deps = try await store.getDependencies(filePath: "Sources/Auth.swift", repoPath: "/path/to/repo")
```

## Protocols

### EmbeddingProvider

Implement this to supply vector embeddings from any backend (MLX, OpenAI, CoreML, etc.):

```swift
public protocol EmbeddingProvider: Sendable {
  func embed(texts: [String]) async throws -> [[Float]]
  var dimensions: Int { get }
  var modelName: String { get }
}
```

### ChunkAnalyzer

Optional — provides AI-generated summaries and tags for code chunks:

```swift
public protocol ChunkAnalyzer: Sendable {
  func analyze(chunk: String, constructType: String?, constructName: String?, language: String?) async throws -> ChunkAnalysis
}
```

### MemoryPressureMonitor

Optional — integrates with platform memory management (e.g., MLX GPU memory):

```swift
public protocol MemoryPressureMonitor: Sendable {
  func isMemoryPressureHigh() -> Bool
  func clearCaches() async
  func memoryDescription() -> String
}
```

## License

MIT — see [LICENSE](LICENSE).
