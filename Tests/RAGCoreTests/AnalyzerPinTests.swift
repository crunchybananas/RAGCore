@testable import RAGCore
import Foundation
import Testing

/// The per-repo analyzer pin and the drift it exposes.
///
/// Context: a corpus accumulates analysis from whatever model happened to be
/// configured at the time. Because each chunk is analyzed once, the models end
/// up *partitioned across* the corpus rather than layered per chunk — one
/// repository whose summaries came from several models of very different
/// quality, all ranked against each other as equals. The pin records which
/// analyzer a corpus is supposed to be built with so the gap is measurable,
/// and so a peer pulling the corpus can extend it with the same model instead
/// of adding another stratum.
private struct StubEmbeddingProvider: EmbeddingProvider {
  var dimensions: Int { 3 }
  var modelName: String { "stub" }
  func embed(texts: [String]) async throws -> [[Float]] { texts.map { _ in [0, 0, 0] } }
}

@Suite("Analyzer pin")
struct AnalyzerPinTests {

  private func makeStore() async throws -> RAGStore {
    let dbURL = FileManager.default.temporaryDirectory
      .appendingPathComponent("ragcore-analyzer-pin-\(UUID().uuidString).sqlite")
    let store = RAGStore(dbURL: dbURL, embeddingProvider: StubEmbeddingProvider())
    try await store.openIfNeeded()
    try await store.ensureSchema()
    return store
  }

  /// Seed one repo at `/repo/alpha` whose chunks are analyzed by the given
  /// models — `models` maps an analyzer name to how many chunks it produced.
  /// A nil model seeds an unanalyzed chunk.
  private func seed(
    _ store: RAGStore,
    models: [(model: String?, count: Int)]
  ) async throws {
    try await store.upsertRepo(id: "r1", name: "alpha", rootPath: "/repo/alpha",
                               lastIndexedAt: nil, repoIdentifier: "github.com/x/alpha")
    try await store.upsertFile(id: "f1", repoId: "r1", path: "a.swift", hash: "h",
                               language: "Swift", updatedAt: "2026-01-01",
                               modulePath: nil, featureTags: nil)
    var index = 0
    for entry in models {
      for _ in 0..<entry.count {
        index += 1
        try await store.upsertChunk(
          id: "c\(index)", fileId: "f1", startLine: index, endLine: index,
          text: "code \(index)", tokenCount: 10,
          constructType: "func", constructName: "f\(index)", metadata: nil,
          aiSummary: entry.model == nil ? nil : "summary \(index)",
          aiTags: nil,
          analyzedAt: entry.model == nil ? nil : "2026-01-01",
          analyzerModel: entry.model
        )
      }
    }
  }

  @Test("A repo starts unpinned")
  func startsUnpinned() async throws {
    let store = try await makeStore()
    try await seed(store, models: [("gemma4:26b", 3)])
    #expect(try await store.repoAnalyzerModel(repoPath: "/repo/alpha") == nil)
  }

  @Test("Pin round-trips, and unpins with nil")
  func pinRoundTrips() async throws {
    let store = try await makeStore()
    try await seed(store, models: [("gemma4:26b", 1)])

    try await store.setRepoAnalyzerModel(repoPath: "/repo/alpha", model: "gemma4:26b")
    #expect(try await store.repoAnalyzerModel(repoPath: "/repo/alpha") == "gemma4:26b")

    try await store.setRepoAnalyzerModel(repoPath: "/repo/alpha", model: nil)
    #expect(try await store.repoAnalyzerModel(repoPath: "/repo/alpha") == nil)
  }

  @Test("Blank and whitespace-only pins read as unpinned, not as a model named ''")
  func blankPinIsUnpinned() async throws {
    let store = try await makeStore()
    try await seed(store, models: [("gemma4:26b", 1)])

    try await store.setRepoAnalyzerModel(repoPath: "/repo/alpha", model: "   ")
    #expect(try await store.repoAnalyzerModel(repoPath: "/repo/alpha") == nil)
  }

  @Test("Drift reports the real mix, heaviest model first")
  func reportsMix() async throws {
    let store = try await makeStore()
    try await seed(store, models: [
      ("qwen3-coder:latest", 5),
      ("gemma4:26b", 3),
      ("Qwen2.5-Coder-7B", 1),
    ])

    let drift = try await store.analyzerDrift(repoPath: "/repo/alpha")
    #expect(drift.coverage.map(\.model) == ["qwen3-coder:latest", "gemma4:26b", "Qwen2.5-Coder-7B"])
    #expect(drift.coverage.map(\.count) == [5, 3, 1])
    #expect(drift.isDrifted)
  }

  @Test("A single-model corpus is not drifted")
  func singleModelIsNotDrifted() async throws {
    let store = try await makeStore()
    try await seed(store, models: [("gemma4:26b", 4)])

    let drift = try await store.analyzerDrift(repoPath: "/repo/alpha")
    #expect(!drift.isDrifted)
    #expect(drift.coverage.count == 1)
  }

  /// The number that made the UI untrustworthy: failures are stored under a
  /// pseudo-model, so counting them as an analyzer inflates "analyzed" and
  /// hides that a large slice of the corpus has no usable summary.
  @Test("Failed analyses are counted separately, never as an analyzer")
  func failuresAreNotAModel() async throws {
    let store = try await makeStore()
    try await seed(store, models: [
      ("gemma4:26b", 2),
      ("chunk-analyzer-failed", 7),
    ])

    let drift = try await store.analyzerDrift(repoPath: "/repo/alpha")
    #expect(drift.failedChunks == 7)
    #expect(drift.coverage.map(\.model) == ["gemma4:26b"])
    #expect(!drift.isDrifted, "one real analyzer plus failures is not a blend")
  }

  @Test("Unanalyzed chunks are reported and excluded from coverage")
  func unanalyzedAreSeparate() async throws {
    let store = try await makeStore()
    try await seed(store, models: [("gemma4:26b", 2), (nil, 5)])

    let drift = try await store.analyzerDrift(repoPath: "/repo/alpha")
    #expect(drift.unanalyzedChunks == 5)
    #expect(drift.coverage.map(\.count) == [2])
  }

  @Test("Off-pin count is the work of converging to the pin")
  func offPinMeasuresTheGap() async throws {
    let store = try await makeStore()
    try await seed(store, models: [
      ("qwen3-coder:latest", 5),
      ("gemma4:26b", 3),
      ("chunk-analyzer-failed", 2),
    ])

    try await store.setRepoAnalyzerModel(repoPath: "/repo/alpha", model: "gemma4:26b")
    let drift = try await store.analyzerDrift(repoPath: "/repo/alpha")

    #expect(drift.pinnedModel == "gemma4:26b")
    #expect(drift.offPinChunks == 5, "only the qwen-analyzed chunks are off-pin")
  }

  @Test("An unpinned repo reports no off-pin work, however mixed it is")
  func unpinnedHasNoOffPinWork() async throws {
    let store = try await makeStore()
    try await seed(store, models: [("qwen3-coder:latest", 5), ("gemma4:26b", 3)])

    let drift = try await store.analyzerDrift(repoPath: "/repo/alpha")
    #expect(drift.pinnedModel == nil)
    #expect(drift.offPinChunks == 0, "with no pin there is nothing to converge toward")
    #expect(drift.isDrifted, "…but the corpus is still visibly a blend")
  }

  @Test("Pinning to the model already covering the corpus leaves no gap")
  func pinningTheIncumbentIsANoOp() async throws {
    let store = try await makeStore()
    try await seed(store, models: [("gemma4:26b", 6)])

    try await store.setRepoAnalyzerModel(repoPath: "/repo/alpha", model: "gemma4:26b")
    let drift = try await store.analyzerDrift(repoPath: "/repo/alpha")
    #expect(drift.offPinChunks == 0)
  }
}
