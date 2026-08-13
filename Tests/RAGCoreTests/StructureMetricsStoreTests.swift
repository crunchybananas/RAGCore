//
//  StructureMetricsStoreTests.swift
//  RAGCoreTests
//
//  End-to-end: index a repo, then ask a structural question of it.
//

import XCTest
@testable import RAGCore

/// Embeds nothing. These tests exercise indexing, storage, and structural
/// queries, none of which touch vectors — and a real provider would make the
/// suite depend on a running model.
private struct NullEmbeddingProvider: EmbeddingProvider {
  var dimensions: Int { 8 }
  var modelName: String { "null-test-provider" }
  func embed(texts: [String]) async throws -> [[Float]] {
    texts.map { _ in [Float](repeating: 0, count: 8) }
  }
}

final class StructureMetricsStoreTests: XCTestCase {

  private var repoURL: URL!
  private var dbURL: URL!

  override func setUpWithError() throws {
    let root = URL(fileURLWithPath: NSTemporaryDirectory())
      .appendingPathComponent("ragcore-structure-\(UUID().uuidString)")
    repoURL = root.appendingPathComponent("repo")
    dbURL = root.appendingPathComponent("rag.sqlite")
    try FileManager.default.createDirectory(at: repoURL, withIntermediateDirectories: true)
    // A git dir makes the scanner treat this as a real repo root.
    try FileManager.default.createDirectory(
      at: repoURL.appendingPathComponent(".git"), withIntermediateDirectories: true)
  }

  override func tearDownWithError() throws {
    try? FileManager.default.removeItem(at: repoURL.deletingLastPathComponent())
  }

  private func write(_ name: String, _ contents: String) throws {
    try contents.write(to: repoURL.appendingPathComponent(name), atomically: true, encoding: .utf8)
  }

  private func makeStore() -> RAGStore {
    RAGStore(dbURL: dbURL, embeddingProvider: NullEmbeddingProvider())
  }

  /// The query the whole feature exists for: find cleanup candidates without
  /// describing them in prose.
  func testIndexThenQueryForCommentedOutCode() async throws {
    try write("Abandoned.swift", """
      func live() {
        run()
      }

      // func old(_ input: String) -> Int {
      //   let parsed = Int(input)
      //   return parsed ?? 0
      // }
      """)
    try write("Clean.swift", """
      /// Adds two numbers.
      func add(_ a: Int, _ b: Int) -> Int {
        a + b
      }
      """)

    let store = makeStore()
    _ = try await store.indexRepository(path: repoURL.path)

    let result = try await store.findStructuralChunks(
      repoPath: repoURL.path, requireCommentedOutCode: true, limit: 20)

    XCTAssertFalse(result.matches.isEmpty, "the commented-out function should be found")
    XCTAssertTrue(result.matches.allSatisfy { $0.metrics.containsCommentedOutCode })
    XCTAssertTrue(
      result.matches.contains { $0.relativePath.contains("Abandoned") },
      "Abandoned.swift holds the commented-out block")
    XCTAssertFalse(
      result.matches.contains { $0.relativePath.contains("Clean") },
      "a doc comment is prose, not abandoned code")
  }

  func testRatioFilterSelectsCommentHeavyChunks() async throws {
    try write("Heavy.swift", """
      // one
      // two
      // three
      // four
      // five
      let x = 1
      """)
    try write("Light.swift", """
      let a = 1
      let b = 2
      let c = 3
      let d = 4
      """)

    let store = makeStore()
    _ = try await store.indexRepository(path: repoURL.path)

    let result = try await store.findStructuralChunks(
      repoPath: repoURL.path, minCommentRatio: 0.5, limit: 20)

    XCTAssertTrue(result.matches.contains { $0.relativePath.contains("Heavy") })
    XCTAssertFalse(result.matches.contains { $0.relativePath.contains("Light") })
  }

  func testCommentBlockLengthFilter() async throws {
    try write("Blocky.swift", """
      // a
      // b
      // c
      // d
      func f() {}
      """)

    let store = makeStore()
    _ = try await store.indexRepository(path: repoURL.path)

    let reachable = try await store.findStructuralChunks(
      repoPath: repoURL.path, minCommentBlockLines: 4, limit: 20)
    let unreachable = try await store.findStructuralChunks(
      repoPath: repoURL.path, minCommentBlockLines: 99, limit: 20)

    XCTAssertFalse(reachable.matches.isEmpty)
    XCTAssertTrue(unreachable.matches.isEmpty)
  }

  /// Metrics must be written by indexing, not left for a later backfill.
  func testIndexingPopulatesMetricsSoNothingIsUnmeasured() async throws {
    try write("A.swift", "// note\nlet x = 1\n")

    let store = makeStore()
    _ = try await store.indexRepository(path: repoURL.path)

    let result = try await store.findStructuralChunks(repoPath: repoURL.path, limit: 20)
    XCTAssertTrue(result.isComplete, "a freshly indexed repo has no unmeasured chunks")
    XCTAssertEqual(result.unmeasuredChunks, 0)
  }

  /// A language with no comment syntax stays NULL rather than being recorded as
  /// zero comments — otherwise it would win every "least commented" ranking.
  func testProseFilesAreNotMeasuredAsZero() async throws {
    try write("README.md", "# Title\n\nSome prose about the project.\n")

    let store = makeStore()
    _ = try await store.indexRepository(path: repoURL.path)

    let result = try await store.findStructuralChunks(repoPath: repoURL.path, limit: 20)
    XCTAssertTrue(
      result.matches.allSatisfy { !$0.relativePath.hasSuffix(".md") },
      "markdown has no comment concept and must not appear as measured")
    XCTAssertGreaterThan(
      result.unmeasuredChunks, 0,
      "it should be reported as unmeasured rather than silently dropped")
  }

  /// Backfill is the path for indexes written before schema v19.
  func testBackfillMeasuresChunksLeftNull() async throws {
    try write("A.swift", "// note\n// more\nlet x = 1\n")

    let store = makeStore()
    _ = try await store.indexRepository(path: repoURL.path)

    // Simulate a pre-v19 index: clear what indexing just wrote.
    try await store.clearStructureMetricsForTesting()
    let cleared = try await store.findStructuralChunks(repoPath: repoURL.path, limit: 20)
    XCTAssertTrue(cleared.matches.isEmpty,
                  "cleared metrics means a structural query finds nothing")

    let report = try await store.backfillStructureMetrics(repoPath: repoURL.path)
    XCTAssertGreaterThan(report.measured, 0)
    XCTAssertTrue(report.isComplete)

    let result = try await store.findStructuralChunks(repoPath: repoURL.path, limit: 20)
    XCTAssertFalse(result.matches.isEmpty, "backfilled metrics are queryable")
    XCTAssertTrue(result.isComplete)
  }

  /// Ordinary search results carry the metrics too, so a caller can rank or
  /// filter a normal query by comment shape without a second round trip.
  func testSearchResultsCarryStructureMetrics() async throws {
    try write("Documented.swift", """
      // configure the widget
      // with sensible defaults
      func configureWidget() {
        apply()
      }
      """)

    let store = makeStore()
    _ = try await store.indexRepository(path: repoURL.path)

    let hits = try await store.search(query: "configureWidget", repoPath: repoURL.path, limit: 10)

    let match = try XCTUnwrap(hits.first { $0.filePath.contains("Documented") })
    let structure = try XCTUnwrap(match.structure, "a Swift chunk must be measured")
    XCTAssertGreaterThan(structure.commentLines, 0)
    XCTAssertGreaterThan(structure.codeLines, 0)
    XCTAssertNotNil(structure.commentLineRatio)
  }

  /// The honest-degradation contract: a query over a partially-measured index
  /// says how much it could not see, instead of returning a short list that
  /// reads as complete.
  func testPartiallyMeasuredIndexReportsUnmeasuredCount() async throws {
    try write("A.swift", "// note\nlet x = 1\n")
    try write("B.swift", "// other\nlet y = 2\n")

    let store = makeStore()
    _ = try await store.indexRepository(path: repoURL.path)
    try await store.clearStructureMetricsForTesting()

    let result = try await store.findStructuralChunks(repoPath: repoURL.path, limit: 20)
    XCTAssertTrue(result.matches.isEmpty)
    XCTAssertGreaterThan(result.unmeasuredChunks, 0)
    XCTAssertFalse(result.isComplete, "an unmeasured population is not completeness")
  }
}
