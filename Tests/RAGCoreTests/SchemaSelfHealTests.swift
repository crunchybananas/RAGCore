@testable import RAGCore
import Foundation
import Testing

private struct SchemaTestEmbeddingProvider: EmbeddingProvider {
  let dimensions = 3
  let modelName = "schema-test"

  func embed(texts: [String]) async throws -> [[Float]] {
    texts.map { _ in [1, 0, 0] }
  }
}

@Suite("Schema self-heal")
struct SchemaSelfHealTests {
  private static let coreJoinIndexes = ["idx_files_repo", "idx_chunks_file", "idx_files_hash"]

  private func makeStore(databaseURL: URL) -> RAGStore {
    RAGStore(dbURL: databaseURL, embeddingProvider: SchemaTestEmbeddingProvider())
  }

  private func indexExists(_ store: RAGStore, name: String) async throws -> Bool {
    try await store.queryInt(
      "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='\(name)'"
    ) > 0
  }

  private func schemaVersion(_ store: RAGStore) async throws -> Int {
    try await store.queryInt(
      "SELECT CAST(value AS INTEGER) FROM rag_meta WHERE key = 'schema_version'"
    )
  }

  /// A database can reach a high schema_version without the core join indexes
  /// (table-rebuild migrations drop them; sync-imported databases are stamped
  /// with a high version so the v2 gate never fires). ensureSchema must
  /// re-assert them on every open, not only when migrating through v2.
  @Test("ensureSchema recreates core join indexes on a high-version database")
  func rehealsDroppedCoreJoinIndexes() async throws {
    let databaseURL = FileManager.default.temporaryDirectory
      .appendingPathComponent("ragcore-schema-selfheal-\(UUID().uuidString).sqlite")
    defer { try? FileManager.default.removeItem(at: databaseURL) }

    // Build a database at the current schema version.
    let store = makeStore(databaseURL: databaseURL)
    try await store.openIfNeeded()
    try await store.ensureSchema()
    let migratedVersion = try await schemaVersion(store)
    #expect(migratedVersion >= 18)
    for name in Self.coreJoinIndexes {
      #expect(try await indexExists(store, name: name), "expected \(name) after migration")
    }

    // Simulate the broken fleet shape: high schema_version, no join indexes.
    for name in Self.coreJoinIndexes {
      try await store.exec("DROP INDEX \(name)")
      #expect(try await indexExists(store, name: name) == false, "expected \(name) dropped")
    }
    await store.closeDatabase()

    // A plain reopen must heal the indexes without touching the version.
    let reopened = makeStore(databaseURL: databaseURL)
    try await reopened.openIfNeeded()
    try await reopened.ensureSchema()
    for name in Self.coreJoinIndexes {
      #expect(try await indexExists(reopened, name: name), "expected \(name) healed on reopen")
    }
    #expect(try await schemaVersion(reopened) == migratedVersion)
    await reopened.closeDatabase()
  }
}
