//
//  TextIndexFootprintTests.swift
//  RAGCoreTests
//
//  Measures the FTS5 lexical index's cost on a real corpus — this package's
//  own Sources tree — and prints it, so size and indexing-time regressions
//  are a test-log grep away instead of a surprise on someone's machine
//  (cloke/peel#2211 acceptance: "index size and indexing time regression
//  measured and reported").
//

@testable import RAGCore
import Foundation
import Testing

private struct FootprintEmbeddingProvider: EmbeddingProvider {
  var dimensions: Int { 3 }
  var modelName: String { "footprint-stub" }
  func embed(texts: [String]) async throws -> [[Float]] { texts.map { _ in [0, 0, 0] } }
}

@Suite("FTS index footprint")
struct TextIndexFootprintTests {

  @Test("index size and time on this package's own sources")
  func measureFootprint() async throws {
    let sourcesURL = URL(fileURLWithPath: #filePath)
      .deletingLastPathComponent()  // RAGCoreTests
      .deletingLastPathComponent()  // Tests
      .deletingLastPathComponent()  // package root
      .appendingPathComponent("Sources")
    let databaseURL = FileManager.default.temporaryDirectory
      .appendingPathComponent("ragcore-fts-footprint-\(UUID().uuidString).sqlite")
    defer { try? FileManager.default.removeItem(at: databaseURL) }

    let store = RAGStore(dbURL: databaseURL, embeddingProvider: FootprintEmbeddingProvider())
    let report = try await store.indexRepository(path: sourcesURL.path)
    #expect(report.chunksIndexed > 0)

    // The contentless index stores postings only; its payload lives in the
    // chunks_fts_data shadow table.
    let ftsBytes = try await store.queryInt(
      "SELECT COALESCE(SUM(LENGTH(block)), 0) FROM chunks_fts_data"
    )
    let chunkBytes = try await store.queryInt(
      "SELECT COALESCE(SUM(LENGTH(text)), 0) FROM chunks"
    )
    let databaseBytes = (try FileManager.default
      .attributesOfItem(atPath: databaseURL.path)[.size] as? Int) ?? 0
    #expect(ftsBytes > 0, "indexing must have populated the lexical index")

    let share = databaseBytes > 0
      ? String(format: "%.1f%%", Double(ftsBytes) / Double(databaseBytes) * 100)
      : "n/a"
    print("""
      [fts-footprint] files=\(report.filesIndexed) chunks=\(report.chunksIndexed) \
      indexMs=\(report.durationMs) dbBytes=\(databaseBytes) ftsBytes=\(ftsBytes) \
      chunkTextBytes=\(chunkBytes) ftsShareOfDb=\(share)
      """)
  }
}
