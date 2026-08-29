//
//  TextIndexFTSTests.swift
//  RAGCoreTests
//
//  The FTS5 lexical index (cloke/peel#2211): BM25 ranking, code-aware term
//  expansion, trigger maintenance across the chunk write paths, and the
//  coverage gate that keeps an unbackfilled repo serving substring — flagged —
//  instead of pretending alphabetical truncation is ranking.
//

@testable import RAGCore
import Foundation
import Testing

private struct FTSStubEmbeddingProvider: EmbeddingProvider {
  var dimensions: Int { 3 }
  var modelName: String { "stub" }
  func embed(texts: [String]) async throws -> [[Float]] { texts.map { _ in [0, 0, 0] } }
}

@Suite("FTS5 lexical index")
struct TextIndexFTSTests {

  private func makeStore() async throws -> RAGStore {
    let dbURL = FileManager.default.temporaryDirectory
      .appendingPathComponent("ragcore-fts-\(UUID().uuidString).sqlite")
    let store = RAGStore(dbURL: dbURL, embeddingProvider: FTSStubEmbeddingProvider())
    try await store.openIfNeeded()
    try await store.ensureSchema()
    return store
  }

  /// One repo, three chunks: a camelCase construct, a body-text mention of
  /// the same identifier, and an unrelated chunk.
  private func seed(_ store: RAGStore) async throws {
    try await store.upsertRepo(
      id: "r", name: "repo", rootPath: "/r",
      lastIndexedAt: nil, repoIdentifier: "github.com/x/r"
    )
    try await store.upsertFile(
      id: "f1", repoId: "r", path: "Sources/App/RepoResolver.swift", hash: "h1",
      language: "swift", updatedAt: "2026-01-01", modulePath: "Sources/App", featureTags: nil
    )
    try await store.upsertChunk(
      id: "c1", fileId: "f1", startLine: 1, endLine: 4,
      text: "func resolveRepo(for path: String) -> Repo? { registry.lookup(path) }",
      tokenCount: 12, constructType: "function", constructName: "resolveRepo", metadata: nil
    )
    try await store.upsertFile(
      id: "f2", repoId: "r", path: "Sources/App/CallSite.swift", hash: "h2",
      language: "swift", updatedAt: "2026-01-01", modulePath: "Sources/App", featureTags: nil
    )
    try await store.upsertChunk(
      id: "c2", fileId: "f2", startLine: 10, endLine: 12,
      text: "let repo = resolveRepo(for: checkoutPath) // uses the resolver",
      tokenCount: 9, constructType: nil, constructName: nil, metadata: nil
    )
    try await store.upsertFile(
      id: "f3", repoId: "r", path: "Sources/App/Unrelated.swift", hash: "h3",
      language: "swift", updatedAt: "2026-01-01", modulePath: "Sources/App", featureTags: nil
    )
    try await store.upsertChunk(
      id: "c3", fileId: "f3", startLine: 1, endLine: 2,
      text: "let zebra = alphabet.shuffled()",
      tokenCount: 5, constructType: nil, constructName: nil, metadata: nil
    )
    try await store.markTextIndexComplete(repoId: "r")
  }

  // MARK: - Tokenization units

  @Test("camelParts splits humps and acronym boundaries")
  func camelPartsSplits() {
    #expect(CodeTokens.camelParts("resolveRepo") == ["resolve", "repo"])
    #expect(CodeTokens.camelParts("HTTPServer") == ["http", "server"])
    #expect(CodeTokens.camelParts("plain") == ["plain"])
    #expect(CodeTokens.camelParts("Repo") == ["repo"])
  }

  @Test("indexText appends splits without disturbing the original")
  func indexTextAppends() {
    let out = CodeTokens.indexText("call resolveRepo() now")
    #expect(out.hasPrefix("call resolveRepo() now"))
    #expect(out.hasSuffix("resolve repo"))
    // No camelCase → unchanged, no trailing noise.
    #expect(CodeTokens.indexText("plain words only") == "plain words only")
  }

  @Test("matchExpression quotes terms so FTS5 operators are literals")
  func matchExpressionNeutralizesOperators() {
    let expr = CodeTokens.matchExpression(for: "repo NEAR term-x", matchAll: true)
    #expect(expr == "\"repo\" AND \"NEAR\" AND \"term-x\"")
    // Pure punctuation has nothing to rank.
    #expect(CodeTokens.matchExpression(for: "-- ***", matchAll: true) == nil)
    // camelCase words expand to the exact form OR the split phrase.
    let camel = CodeTokens.matchExpression(for: "resolveRepo", matchAll: true)
    #expect(camel == "(\"resolveRepo\" OR \"resolve repo\")")
  }

  // MARK: - Ranking

  @Test("split-term query finds a camelCase identifier via BM25")
  func splitTermsFindCamelCase() async throws {
    let store = try await makeStore()
    try await seed(store)
    let outcome = try await store.searchText(query: "resolve repo", repoPath: "/r", limit: 10)
    #expect(outcome.ranking == .bm25)
    #expect(outcome.pendingTextIndexRepos == 0)
    #expect(outcome.results.contains { $0.constructName == "resolveRepo" })
  }

  @Test("construct-name hit outranks a body-text hit")
  func constructNameOutranksBody() async throws {
    let store = try await makeStore()
    try await seed(store)
    let outcome = try await store.searchText(query: "resolveRepo", repoPath: "/r", limit: 10)
    #expect(outcome.ranking == .bm25)
    #expect(outcome.results.count >= 2)
    #expect(
      outcome.results.first?.constructName == "resolveRepo",
      "the chunk NAMED resolveRepo must beat the chunk merely mentioning it"
    )
    // BM25 scores surface as higher-is-better.
    if outcome.results.count >= 2,
       let first = outcome.results[0].score, let second = outcome.results[1].score {
      #expect(first >= second)
    }
  }

  @Test("matchAll AND excludes partial matches; OR includes them")
  func matchAllSemantics() async throws {
    let store = try await makeStore()
    try await seed(store)
    let strict = try await store.searchText(
      query: "zebra resolver", repoPath: "/r", limit: 10, matchAll: true
    )
    #expect(strict.results.isEmpty, "no chunk contains both terms")
    let relaxed = try await store.searchText(
      query: "zebra resolver", repoPath: "/r", limit: 10, matchAll: false
    )
    #expect(relaxed.results.count >= 2)
  }

  // MARK: - Trigger maintenance

  @Test("pruning a file removes its postings")
  func pruneRemovesPostings() async throws {
    let store = try await makeStore()
    try await seed(store)
    _ = try await store.pruneDeletedFiles(
      repoId: "r",
      currentPaths: ["Sources/App/CallSite.swift", "Sources/App/Unrelated.swift"]
    )
    let outcome = try await store.searchText(query: "resolveRepo", repoPath: "/r", limit: 10)
    #expect(outcome.ranking == .bm25)
    #expect(!outcome.results.contains { $0.constructName == "resolveRepo" })
  }

  @Test("replacing a chunk replaces its postings, leaving none stale")
  func replaceUpdatesPostings() async throws {
    let store = try await makeStore()
    try await seed(store)
    try await store.upsertChunk(
      id: "c3", fileId: "f3", startLine: 1, endLine: 2,
      text: "let giraffe = savanna.tallest()",
      tokenCount: 5, constructType: nil, constructName: nil, metadata: nil
    )
    let stale = try await store.searchText(query: "zebra", repoPath: "/r", limit: 10)
    #expect(stale.results.isEmpty, "the replaced text must leave the index")
    let fresh = try await store.searchText(query: "giraffe", repoPath: "/r", limit: 10)
    #expect(fresh.results.count == 1)
    #expect(fresh.ranking == .bm25)
  }

  @Test("an ai_summary update becomes searchable")
  func summaryUpdateIndexes() async throws {
    let store = try await makeStore()
    try await seed(store)
    try await store.exec(
      "UPDATE chunks SET ai_summary = 'quantum flux capacitor summary' WHERE id = 'c3'"
    )
    let outcome = try await store.searchText(query: "quantum", repoPath: "/r", limit: 10)
    #expect(outcome.ranking == .bm25)
    #expect(outcome.results.count == 1)
  }

  // MARK: - Coverage gate

  @Test("an unbackfilled repo serves substring, flagged, until rebuilt")
  func coverageGateIsHonest() async throws {
    let store = try await makeStore()
    try await seed(store)
    // Simulate a pre-v23 database: postings gone, no coverage verdict.
    try await store.exec("DELETE FROM chunks_fts")
    try await store.exec("DELETE FROM fts_repo_state")

    let degraded = try await store.searchText(query: "resolveRepo", repoPath: "/r", limit: 10)
    #expect(degraded.ranking == .substring)
    #expect(degraded.pendingTextIndexRepos == 1)
    #expect(
      degraded.results.contains { $0.constructName == "resolveRepo" },
      "substring still finds it — degraded means unranked, not empty"
    )

    let backfilled = try await store.rebuildTextIndex(repoPath: "/r")
    #expect(backfilled == 3)
    let restored = try await store.searchText(query: "resolveRepo", repoPath: "/r", limit: 10)
    #expect(restored.ranking == .bm25)
    #expect(restored.pendingTextIndexRepos == 0)
  }

  @Test("coverage self-heals for a repo whose chunks all arrived via triggers")
  func coverageSelfVerifies() async throws {
    let store = try await makeStore()
    try await seed(store)
    // Wipe only the verdict: postings are intact because the triggers wrote
    // them, so one verification pass should restore bm25 with no rebuild.
    try await store.exec("DELETE FROM fts_repo_state")
    let outcome = try await store.searchText(query: "resolveRepo", repoPath: "/r", limit: 10)
    #expect(outcome.ranking == .bm25)
    #expect(outcome.pendingTextIndexRepos == 0)
  }

  // MARK: - Explicit substring mode

  @Test("searchSubstring matches mid-identifier fragments")
  func substringModeStillAvailable() async throws {
    let store = try await makeStore()
    try await seed(store)
    let results = try await store.searchSubstring(query: "olveRe", repoPath: "/r", limit: 10)
    #expect(results.contains { $0.constructName == "resolveRepo" })
    // BM25 would not match a fragment; that is what this mode is for.
    let ranked = try await store.searchText(query: "olveRe", repoPath: "/r", limit: 10)
    #expect(ranked.ranking == .bm25)
    #expect(ranked.results.isEmpty)
  }
}
