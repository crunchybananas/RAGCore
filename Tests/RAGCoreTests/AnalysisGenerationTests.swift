@testable import RAGCore
import Foundation
import Testing

private struct GenerationEmbeddingProvider: EmbeddingProvider {
  let dimensions = 3
  let modelName = "generation-embedder"

  func embed(texts: [String]) async throws -> [[Float]] {
    texts.map { _ in [0, 1, 0] }
  }
}

private struct GenerationAnalyzer: ChunkAnalyzer {
  let analyzerName = "replacement-analyzer"

  func analyze(
    chunk: String,
    constructType: String?,
    constructName: String?,
    language: String?
  ) async throws -> ChunkAnalysis {
    ChunkAnalysis(
      summary: "replacement summary for \(constructName ?? chunk)",
      tags: ["replacement", language ?? "unknown"]
    )
  }
}

@Suite("Blue/green analysis generations")
struct AnalysisGenerationTests {
  private func makeStore(databaseURL: URL) async throws -> RAGStore {
    let store = RAGStore(
      dbURL: databaseURL,
      embeddingProvider: GenerationEmbeddingProvider(),
      chunkAnalyzer: GenerationAnalyzer()
    )
    _ = try await store.initialize()
    return store
  }

  private func seed(_ store: RAGStore) async throws {
    try await store.upsertRepo(
      id: "r1", name: "alpha", rootPath: "/repo/alpha",
      lastIndexedAt: nil, repoIdentifier: "github.com/x/alpha",
      embeddingModel: "generation-embedder", embeddingDimensions: 3
    )
    try await store.upsertFile(
      id: "f1", repoId: "r1", path: "A.swift", hash: "h1",
      language: "Swift", updatedAt: "2026-01-01",
      modulePath: nil, featureTags: nil
    )
    for index in 1...2 {
      try await store.upsertChunk(
        id: "c\(index)", fileId: "f1", startLine: index, endLine: index,
        text: "func value\(index)() {}", tokenCount: 5,
        constructType: "function", constructName: "value\(index)", metadata: nil,
        aiSummary: "incumbent summary \(index)", aiTags: "[\"incumbent\"]",
        analyzedAt: "2026-01-01", analyzerModel: "incumbent-analyzer"
      )
      try await store.exec(
        "UPDATE chunks SET enriched_at = '2026-01-01' WHERE id = 'c\(index)'"
      )
      try await store.upsertEmbedding(chunkId: "c\(index)", vector: [1, 0, 0])
    }
    try await store.setRepoAnalyzerModel(repoPath: "/repo/alpha", model: "incumbent-analyzer")
  }

  private func databaseURL() -> URL {
    FileManager.default.temporaryDirectory
      .appendingPathComponent("ragcore-analysis-generation-\(UUID().uuidString).sqlite")
  }

  @Test("Staging survives restart and never changes active search")
  func stagingResumesWithoutChangingActiveSearch() async throws {
    let url = databaseURL()
    defer { try? FileManager.default.removeItem(at: url) }
    let first = try await makeStore(databaseURL: url)
    try await seed(first)

    let started = try await first.beginAnalysisGeneration(
      repoPath: "/repo/alpha", analyzerModel: "replacement-analyzer")
    #expect(try await first.analyzeGeneration(generationId: started.generationId, limit: 1) == 1)
    #expect(try await first.search(query: "incumbent", repoPath: "/repo/alpha").count == 2)
    #expect(try await first.search(query: "replacement", repoPath: "/repo/alpha").isEmpty)
    await first.closeDatabase()

    let resumedStore = try await makeStore(databaseURL: url)
    let resumed = try await resumedStore.beginAnalysisGeneration(
      repoPath: "/repo/alpha", analyzerModel: "replacement-analyzer")
    #expect(resumed.generationId == started.generationId)
    #expect(resumed.analyzedChunks == 1)
    #expect(resumed.enrichedChunks == 0)
    try await resumedStore.abortAnalysisGeneration(generationId: resumed.generationId)
  }

  @Test("Commit flips summaries, analyzer pin, and vectors together")
  func commitPromotesTheCompleteGeneration() async throws {
    let url = databaseURL()
    defer { try? FileManager.default.removeItem(at: url) }
    let store = try await makeStore(databaseURL: url)
    try await seed(store)

    let started = try await store.beginAnalysisGeneration(
      repoPath: "/repo/alpha", analyzerModel: "replacement-analyzer")
    #expect(try await store.analyzeGeneration(generationId: started.generationId, limit: 10) == 2)
    #expect(try await store.enrichAnalysisGeneration(generationId: started.generationId, limit: 10) == 2)

    let ready = try await store.analysisGenerationStatus(generationId: started.generationId)
    #expect(ready.phase == .ready)
    #expect(ready.isReady)
    #expect(try await store.search(query: "incumbent", repoPath: "/repo/alpha").count == 2)
    #expect(try await store.search(query: "replacement", repoPath: "/repo/alpha").isEmpty)
    #expect(try await store.searchVectorWithEmbedding(
      [1, 0, 0], repoPath: "/repo/alpha", limit: 10, threshold: 0.99
    ).count == 2)

    let committed = try await store.commitAnalysisGeneration(generationId: started.generationId)
    #expect(committed.phase == .committed)
    #expect(committed.isReady)
    #expect(try await store.search(query: "incumbent", repoPath: "/repo/alpha").isEmpty)
    #expect(try await store.search(query: "replacement", repoPath: "/repo/alpha").count == 2)
    #expect(try await store.searchVectorWithEmbedding(
      [0, 1, 0], repoPath: "/repo/alpha", limit: 10, threshold: 0.99
    ).count == 2)
    #expect(try await store.searchVectorWithEmbedding(
      [1, 0, 0], repoPath: "/repo/alpha", limit: 10, threshold: 0.99
    ).isEmpty)

    let drift = try await store.analyzerDrift(repoPath: "/repo/alpha")
    #expect(drift.pinnedModel == "replacement-analyzer")
    #expect(drift.coverage.map(\.model) == ["replacement-analyzer"])
    #expect(try await store.listRepos().first?.activeAnalysisGeneration == started.generationId)
    #expect(try await store.queryInt("SELECT COUNT(*) FROM staged_chunk_analysis") == 0)
  }

  @Test("Abort drops staging and leaves the incumbent untouched")
  func abortLeavesActiveRowsUntouched() async throws {
    let url = databaseURL()
    defer { try? FileManager.default.removeItem(at: url) }
    let store = try await makeStore(databaseURL: url)
    try await seed(store)

    let started = try await store.beginAnalysisGeneration(
      repoPath: "/repo/alpha", analyzerModel: "replacement-analyzer")
    _ = try await store.analyzeGeneration(generationId: started.generationId, limit: 1)
    try await store.abortAnalysisGeneration(generationId: started.generationId)

    #expect(try await store.queryInt("SELECT COUNT(*) FROM analysis_generations") == 0)
    #expect(try await store.queryInt("SELECT COUNT(*) FROM staged_chunk_analysis") == 0)
    #expect(try await store.search(query: "incumbent", repoPath: "/repo/alpha").count == 2)
    #expect(try await store.repoAnalyzerModel(repoPath: "/repo/alpha") == "incumbent-analyzer")
  }

  @Test("A failed promotion rolls every live surface back")
  func commitFailureIsAtomic() async throws {
    let url = databaseURL()
    defer { try? FileManager.default.removeItem(at: url) }
    let store = try await makeStore(databaseURL: url)
    try await seed(store)

    let started = try await store.beginAnalysisGeneration(
      repoPath: "/repo/alpha", analyzerModel: "replacement-analyzer")
    _ = try await store.analyzeGeneration(generationId: started.generationId, limit: 10)
    _ = try await store.enrichAnalysisGeneration(generationId: started.generationId, limit: 10)
    try await store.exec("""
      CREATE TRIGGER reject_generation_commit
      BEFORE UPDATE OF ai_summary ON chunks
      WHEN NEW.id = 'c2'
      BEGIN SELECT RAISE(ABORT, 'injected commit failure'); END
      """)

    do {
      _ = try await store.commitAnalysisGeneration(generationId: started.generationId)
      Issue.record("Expected the injected trigger to abort generation promotion")
    } catch {
      // Expected. The assertions below prove the rollback boundary.
    }

    #expect(try await store.search(query: "incumbent", repoPath: "/repo/alpha").count == 2)
    #expect(try await store.search(query: "replacement", repoPath: "/repo/alpha").isEmpty)
    #expect(try await store.repoAnalyzerModel(repoPath: "/repo/alpha") == "incumbent-analyzer")
    #expect(try await store.listRepos().first?.activeAnalysisGeneration == nil)
    let stillReady = try await store.analysisGenerationStatus(generationId: started.generationId)
    #expect(stillReady.phase == .ready)
    #expect(stillReady.isReady)
  }
}
