@testable import RAGCore
import Foundation
import Testing

/// Regression tests for the opt-in `root_path` remap in `resolveRepo(for:)`.
///
/// Before #2, any identifier-matched resolution rewrote the repo's stored
/// `root_path` to the queried path — on the READ path — so a nested, mistyped,
/// or foreign query that shared a remote identifier silently corrupted the
/// canonical location. The remap is now opt-in (`remapRootPathOnMismatch`), which
/// only indexing sets; read/query callers keep the default and never mutate.
private struct StubEmbeddingProvider: EmbeddingProvider {
  var dimensions: Int { 3 }
  var modelName: String { "stub" }
  func embed(texts: [String]) async throws -> [[Float]] { texts.map { _ in [0, 0, 0] } }
}

@Suite("resolveRepo root_path remap (#2)")
struct ResolveRepoRemapTests {

  private func makeStore() async throws -> RAGStore {
    let dbURL = FileManager.default.temporaryDirectory
      .appendingPathComponent("ragcore-remap-\(UUID().uuidString).sqlite")
    let store = RAGStore(dbURL: dbURL, embeddingProvider: StubEmbeddingProvider())
    try await store.openIfNeeded()
    try await store.ensureSchema()
    return store
  }

  @Test("read path does NOT remap root_path on an identifier-match mismatch")
  func readPathDoesNotRemap() async throws {
    let store = try await makeStore()
    try await store.upsertRepo(id: "r1", name: "alpha", rootPath: "/orig/alpha",
                               lastIndexedAt: nil, repoIdentifier: "github.com/x/alpha")
    // Seed the remote-URL cache so resolution matches by identifier without
    // shelling out to git for a real checkout.
    await store.seedRemoteURLCache(path: "/other/alpha", identifier: "github.com/x/alpha")

    let resolved = try await store.resolveRepo(for: "/other/alpha")  // default: remap off
    #expect(resolved?.id == "r1")
    // Returns the STORED canonical path, not the queried one.
    #expect(resolved?.rootPath == "/orig/alpha")
    // And the DB row is untouched.
    let stored = try await store.listRepos().first { $0.id == "r1" }
    #expect(stored?.rootPath == "/orig/alpha")
  }

  @Test("indexing (opt-in) DOES remap root_path")
  func indexPathRemaps() async throws {
    let store = try await makeStore()
    try await store.upsertRepo(id: "r1", name: "alpha", rootPath: "/orig/alpha",
                               lastIndexedAt: nil, repoIdentifier: "github.com/x/alpha")
    await store.seedRemoteURLCache(path: "/moved/alpha", identifier: "github.com/x/alpha")

    let resolved = try await store.resolveRepo(for: "/moved/alpha", remapRootPathOnMismatch: true)
    #expect(resolved?.id == "r1")
    #expect(resolved?.rootPath == "/moved/alpha")
    let stored = try await store.listRepos().first { $0.id == "r1" }
    #expect(stored?.rootPath == "/moved/alpha")
  }
}
