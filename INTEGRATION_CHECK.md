# Paired-package smoke check

November 9, 2026: passed with MCPCore 2.0 and RAGCore 3.0 storage ownership changes, including the bridge header fix. The temporary consumer source is preserved below for reproduction; adjust the two absolute checkout paths for another machine. Run `swift run StorageBoundaryCheck` from the fixture directory. SQLite data stays in that directory.

## Package.swift

```swift
// swift-tools-version: 6.0
import PackageDescription
let package = Package(
  name: "StorageBoundaryCheck",
  platforms: [.macOS("26")],
  dependencies: [
    .package(name: "MCPCore", path: "/Users/coryloken/code/mcpcore"),
    .package(name: "RAGCore", path: "/Users/coryloken/code/RAGCore")
  ],
  targets: [.executableTarget(name: "StorageBoundaryCheck", dependencies: [
    .product(name: "MCPCore", package: "MCPCore"),
    .product(name: "RAGCore", package: "RAGCore"),
    .product(name: "CSQLite", package: "RAGCore")
  ])]
)
```

## Sources/StorageBoundaryCheck/main.swift

```swift
import Foundation
import MCPCore
import RAGCore
import CSQLite

struct StubEmbeddings: EmbeddingProvider {
  let dimensions = 3
  let modelName = "storage-boundary-check"
  func embed(texts: [String]) async throws -> [[Float]] {
    texts.map { _ in [1, 0, 0] }
  }
}

@main struct StorageBoundaryCheck {
  static func main() async throws {
    let request = JSONRPCRequest(id: .string("smoke"), method: "ping")
    let decoded = try JSONDecoder().decode(JSONRPCRequest.self, from: JSONEncoder().encode(request))
    precondition(decoded.id == .string("smoke") && decoded.method == "ping")
    precondition(MCPCoreVersion.major == 2)
    precondition(String(cString: sqlite3_libversion()) == "3.47.2")
    precondition(sqlite3_compileoption_used("ENABLE_FTS5") == 1)
    precondition(sqlite3_compileoption_used("ENABLE_RTREE") == 1)

    let url = URL(fileURLWithPath: FileManager.default.currentDirectoryPath).appendingPathComponent("boundary.sqlite")
    let store = RAGStore(dbURL: url, embeddingProvider: StubEmbeddings())
    let status = try await store.initialize()
    precondition(status.extensionLoaded && status.extensionVersion == "v0.1.9")
    await store.closeDatabase()
    let reopened = try await store.initialize()
    precondition(reopened.extensionLoaded)
    await store.closeDatabase()
    print("PASS: MCPCore 2 + RAGCore 3 + explicit CSQLite import; JSON-RPC round trip; SQLite 3.47.2/FTS5/RTREE; sqlite-vec v0.1.9 initializes and reopens.")
  }
}
```
