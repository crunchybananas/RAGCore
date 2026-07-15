@testable import RAGCore
import Foundation
import Testing

private struct VectorTestEmbeddingProvider: EmbeddingProvider {
  let dimensions = 3
  let modelName = "vector-test"

  func embed(texts: [String]) async throws -> [[Float]] {
    texts.map { _ in [1, 0, 0] }
  }
}

private struct BenchmarkEmbeddingProvider: EmbeddingProvider {
  let dimensions = 768
  let modelName = "benchmark-768"

  func embed(texts: [String]) async throws -> [[Float]] {
    texts.map { _ in [Float](repeating: 0, count: dimensions) }
  }
}

@Suite("statically linked sqlite-vec")
struct SQLiteVecTests {
  private func makeStore() -> (store: RAGStore, databaseURL: URL) {
    let databaseURL = FileManager.default.temporaryDirectory
      .appendingPathComponent("ragcore-sqlite-vec-\(UUID().uuidString).sqlite")
    return (
      RAGStore(dbURL: databaseURL, embeddingProvider: VectorTestEmbeddingProvider()),
      databaseURL
    )
  }

  private func insertChunk(
    store: RAGStore,
    repoID: String,
    rootPath: String,
    fileID: String,
    filePath: String,
    chunkID: String,
    vector: [Float]
  ) async throws {
    try await store.upsertRepo(
      id: repoID,
      name: repoID,
      rootPath: rootPath,
      lastIndexedAt: nil,
      embeddingModel: "vector-test",
      embeddingDimensions: 3
    )
    try await store.upsertFile(
      id: fileID,
      repoId: repoID,
      path: filePath,
      hash: fileID,
      language: "swift",
      updatedAt: "2026-07-15T00:00:00Z",
      modulePath: "Sources/Feature",
      featureTags: nil
    )
    try await store.upsertChunk(
      id: chunkID,
      fileId: fileID,
      startLine: 1,
      endLine: 2,
      text: "func \(chunkID)() {}",
      tokenCount: 4,
      constructType: "function",
      constructName: chunkID,
      metadata: nil
    )
    try await store.upsertEmbedding(chunkId: chunkID, vector: vector)
  }

  @Test("initialization always provides the compiled vector extension")
  func initializesStaticExtension() async throws {
    let (store, databaseURL) = makeStore()
    defer { try? FileManager.default.removeItem(at: databaseURL) }

    let status = try await store.initialize()
    let runtimeVersion = try await store.queryString("SELECT vec_version()")

    #expect(status.extensionLoaded)
    #expect(status.extensionVersion == "v0.1.9")
    #expect(runtimeVersion == "v0.1.9")
  }

  @Test("KNN search partitions by repository and migrates the legacy vec table")
  func knnSearchAndLegacyMigration() async throws {
    let (store, databaseURL) = makeStore()
    defer { try? FileManager.default.removeItem(at: databaseURL) }
    _ = try await store.initialize()

    try await insertChunk(
      store: store,
      repoID: "repo-a",
      rootPath: "/repos/a",
      fileID: "file-a1",
      filePath: "Sources/A1.swift",
      chunkID: "exact-a",
      vector: [1, 0, 0]
    )
    try await insertChunk(
      store: store,
      repoID: "repo-a",
      rootPath: "/repos/a",
      fileID: "file-a2",
      filePath: "Sources/A2.swift",
      chunkID: "near-a",
      vector: [0.8, 0.2, 0]
    )
    try await insertChunk(
      store: store,
      repoID: "repo-b",
      rootPath: "/repos/b",
      fileID: "file-b1",
      filePath: "Sources/B1.swift",
      chunkID: "exact-b",
      vector: [1, 0, 0]
    )

    // Recreate the pre-v2 table shape to prove initialization migrates it and
    // reconstructs repo partition data from the canonical embeddings table.
    try await store.exec("DROP TABLE vec_chunks")
    try await store.exec("DELETE FROM rag_meta WHERE key = 'vec_schema_signature'")
    try await store.exec("""
      CREATE VIRTUAL TABLE vec_chunks USING vec0 (
        chunk_id TEXT PRIMARY KEY,
        embedding float[3]
      )
      """)

    let results = try await store.searchVectorWithEmbedding(
      [1, 0, 0],
      repoPath: "/repos/a",
      limit: 2,
      threshold: 0
    )
    let schemaSignature = try await store.queryString(
      "SELECT value FROM rag_meta WHERE key = 'vec_schema_signature'"
    )
    let vectorCount = try await store.queryInt("SELECT COUNT(*) FROM vec_chunks")

    #expect(results.map(\.filePath) == ["/repos/a/Sources/A1.swift", "/repos/a/Sources/A2.swift"])
    #expect(!results.contains { $0.filePath.contains("/repos/b") })
    #expect(schemaSignature == "v2:3:cosine:repo-partition")
    #expect(vectorCount == 3)
  }

  @Test(
    "10k chunk sqlite-vec benchmark",
    .enabled(if: ProcessInfo.processInfo.environment["RAGCORE_BENCHMARK"] == "1")
  )
  func sqliteVecTenThousandChunkBenchmark() async throws {
    let databaseURL = FileManager.default.temporaryDirectory
      .appendingPathComponent("ragcore-sqlite-vec-benchmark-\(UUID().uuidString).sqlite")
    defer { try? FileManager.default.removeItem(at: databaseURL) }
    let store = RAGStore(dbURL: databaseURL, embeddingProvider: BenchmarkEmbeddingProvider())
    _ = try await store.initialize()

    try await store.upsertRepo(
      id: "benchmark-repo",
      name: "benchmark",
      rootPath: "/benchmark",
      lastIndexedAt: nil,
      embeddingModel: "benchmark-768",
      embeddingDimensions: 768
    )
    try await store.upsertFile(
      id: "benchmark-file",
      repoId: "benchmark-repo",
      path: "Benchmark.swift",
      hash: "benchmark",
      language: "swift",
      updatedAt: "2026-07-15T00:00:00Z",
      modulePath: nil,
      featureTags: nil
    )

    try await store.exec("BEGIN TRANSACTION")
    do {
      for index in 0..<10_000 {
        let chunkID = "chunk-\(index)"
        try await store.upsertChunk(
          id: chunkID,
          fileId: "benchmark-file",
          startLine: index + 1,
          endLine: index + 1,
          text: "func benchmark\(index)() {}",
          tokenCount: 4,
          constructType: "function",
          constructName: "benchmark\(index)",
          metadata: nil
        )
        var vector = [Float](repeating: 0, count: 768)
        vector[index % vector.count] = 1
        try await store.upsertEmbedding(chunkId: chunkID, vector: vector)
      }
      try await store.exec("COMMIT")
    } catch {
      try? await store.exec("ROLLBACK")
      throw error
    }

    var query = [Float](repeating: 0, count: 768)
    query[0] = 1
    _ = try await store.searchVectorAccelerated(
      queryVector: query,
      resolvedRepoId: "benchmark-repo",
      limit: 10,
      threshold: 0,
      modulePath: nil
    )
    _ = try await store.searchVectorBruteForce(
      queryVector: query,
      resolvedRepoId: "benchmark-repo",
      limit: 10,
      threshold: 0,
      modulePath: nil
    )

    let clock = ContinuousClock()
    let acceleratedStart = clock.now
    for _ in 0..<5 {
      _ = try await store.searchVectorAccelerated(
        queryVector: query,
        resolvedRepoId: "benchmark-repo",
        limit: 10,
        threshold: 0,
        modulePath: nil
      )
    }
    let acceleratedDuration = acceleratedStart.duration(to: clock.now)

    let bruteForceStart = clock.now
    for _ in 0..<5 {
      _ = try await store.searchVectorBruteForce(
        queryVector: query,
        resolvedRepoId: "benchmark-repo",
        limit: 10,
        threshold: 0,
        modulePath: nil
      )
    }
    let bruteForceDuration = bruteForceStart.duration(to: clock.now)

    let acceleratedMilliseconds = milliseconds(acceleratedDuration) / 5
    let bruteForceMilliseconds = milliseconds(bruteForceDuration) / 5
    print(
      "RAGCORE_BENCHMARK chunks=10000 dimensions=768 "
        + "sqlite_vec_ms=\(acceleratedMilliseconds) brute_force_ms=\(bruteForceMilliseconds)"
    )
    #expect(acceleratedMilliseconds < bruteForceMilliseconds)
  }

  private func milliseconds(_ duration: Duration) -> Double {
    let components = duration.components
    return Double(components.seconds) * 1_000
      + Double(components.attoseconds) / 1_000_000_000_000_000
  }
}
