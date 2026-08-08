//
//  PruneDeletedFilesIntegrityTests.swift
//  RAGCoreTests
//
//  Pins the one rule every delete path in this store has to follow: remove the
//  child rows yourself.
//
//  Nothing cascades here. The schema declares plain
//  `FOREIGN KEY (file_id) REFERENCES files(id)` with no `ON DELETE CASCADE`,
//  and `PRAGMA foreign_keys` defaults to OFF and is never enabled anywhere in
//  RAGCore. `pruneDeletedFiles` nonetheless deleted the file row and left its
//  chunks and embeddings behind, on the strength of a comment claiming they
//  cascaded. Because pruning runs on every index refresh, that leaked on every
//  renamed or deleted file and accumulated to 11,218 dangling chunks carrying
//  10,906 embeddings on one machine (cloke/peel#1881).
//

@testable import RAGCore
import Foundation
import Testing

private struct StubEmbeddingProvider: EmbeddingProvider {
  var dimensions: Int { 3 }
  var modelName: String { "stub" }
  func embed(texts: [String]) async throws -> [[Float]] { texts.map { _ in [0, 0, 0] } }
}

@Suite("pruneDeletedFiles integrity")
struct PruneDeletedFilesIntegrityTests {

  private func makeStore() async throws -> RAGStore {
    let dbURL = FileManager.default.temporaryDirectory
      .appendingPathComponent("ragcore-prune-integrity-\(UUID().uuidString).sqlite")
    let store = RAGStore(dbURL: dbURL, embeddingProvider: StubEmbeddingProvider())
    try await store.openIfNeeded()
    try await store.ensureSchema()
    return store
  }

  /// Two files, each with a chunk and an embedding.
  private func seed(_ store: RAGStore) async throws {
    try await store.upsertRepo(
      id: "r", name: "repo", rootPath: "/r",
      lastIndexedAt: nil, repoIdentifier: "github.com/x/r"
    )
    for index in 0..<2 {
      try await store.upsertFile(
        id: "f\(index)", repoId: "r", path: "src/file\(index).swift", hash: "h\(index)",
        language: "swift", updatedAt: "2026-01-01", modulePath: nil, featureTags: nil
      )
      try await store.upsertChunk(
        id: "c\(index)", fileId: "f\(index)", startLine: 1, endLine: 2,
        text: "body \(index)", tokenCount: 3,
        constructType: nil, constructName: nil, metadata: nil
      )
      try await store.upsertEmbedding(chunkId: "c\(index)", vector: [0, 0, 0])
    }
  }

  /// THE regression. Prune one file and the store must be left with nothing
  /// dangling. Before the fix this left one chunk and one embedding behind.
  @Test("pruning a stale file takes its chunks and embeddings with it")
  func pruneLeavesNothingDangling() async throws {
    let store = try await makeStore()
    try await seed(store)
    #expect(try await store.integrityReport().isClean)

    // file1 is gone from disk; file0 survives.
    let removed = try await store.pruneDeletedFiles(repoId: "r", currentPaths: ["src/file0.swift"])
    #expect(removed == 1)

    let report = try await store.integrityReport()
    #expect(report.danglingChunks == 0, "the pruned file's chunk must go with it, not outlive it")
    #expect(report.danglingEmbeddings == 0, "and so must its embedding")
    #expect(report.isClean)

    // The surviving file keeps everything it had.
    #expect(try await store.testOnlyFilePaths(repoId: "r") == ["src/file0.swift"])
    #expect(try await store.testOnlyChunkCount(repoId: "r") == 1)
  }

  /// Pruning every file is the same rule at the boundary — an empty repo must
  /// not be an empty repo plus a pile of unreachable rows.
  @Test("pruning every file leaves an empty, clean store")
  func pruningEverythingLeavesNothingBehind() async throws {
    let store = try await makeStore()
    try await seed(store)

    let removed = try await store.pruneDeletedFiles(repoId: "r", currentPaths: [])
    #expect(removed == 2)
    #expect(try await store.integrityReport().isClean)
    #expect(try await store.testOnlyChunkCount(repoId: "r") == 0)
  }

  /// A clean store reports clean, so `isClean` means something.
  @Test("integrityReport reports clean when nothing is orphaned")
  func cleanStoreReportsClean() async throws {
    let store = try await makeStore()
    try await seed(store)

    let report = try await store.integrityReport()
    #expect(report.danglingChunks == 0)
    #expect(report.danglingEmbeddings == 0)
    #expect(report.summary == "No dangling rows")
  }

  /// The sweep for rows already in the store. Orphans are created the way the
  /// bug created them — a bare `DELETE FROM files` — so this exercises the real
  /// shape rather than a synthetic one.
  @Test("deleteDanglingRows removes orphans and reports what it removed")
  func sweepRemovesExistingOrphans() async throws {
    let store = try await makeStore()
    try await seed(store)
    try await store.testOnlyDeleteFileRowOnly(fileId: "f1")

    let before = try await store.integrityReport()
    #expect(before.danglingChunks == 1)
    #expect(before.danglingEmbeddings == 1)
    #expect(!before.isClean)
    #expect(before.summary == "1 dangling chunk(s), 1 dangling embedding(s)")

    let removed = try await store.deleteDanglingRows()
    #expect(removed.danglingChunks == 1, "the sweep reports what it removed")
    #expect(removed.danglingEmbeddings == 1)

    #expect(try await store.integrityReport().isClean)
    // The untouched file is not collateral.
    #expect(try await store.testOnlyChunkCount(repoId: "r") == 1)
  }

  /// Sweeping a clean store is a no-op that says so, rather than reporting
  /// phantom work.
  @Test("deleteDanglingRows on a clean store removes nothing")
  func sweepOnCleanStoreIsANoOp() async throws {
    let store = try await makeStore()
    try await seed(store)

    let removed = try await store.deleteDanglingRows()
    #expect(removed.isClean)
    #expect(try await store.testOnlyChunkCount(repoId: "r") == 2)
  }
}
