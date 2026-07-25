//
//  DeleteFilesUnderTests.swift
//  RAGCoreTests
//
//  Pins the surgical cleanup used to strip submodule content an older indexer
//  wrote into a superproject's row.
//
//  `deleteRepo` is the wrong instrument for that job: the superproject is a real
//  repo with real files of its own, and dropping the row makes it vanish from
//  the repo list and demands a full re-index to bring back. These tests exist to
//  keep this narrow — it must remove the duplicated subtrees and nothing else.
//

@testable import RAGCore
import Foundation
import Testing

private struct StubEmbeddingProvider: EmbeddingProvider {
  var dimensions: Int { 3 }
  var modelName: String { "stub" }
  func embed(texts: [String]) async throws -> [[Float]] { texts.map { _ in [0, 0, 0] } }
}

@Suite("deleteFilesUnder")
struct DeleteFilesUnderTests {

  private func makeStore() async throws -> RAGStore {
    let dbURL = FileManager.default.temporaryDirectory
      .appendingPathComponent("ragcore-delfiles-\(UUID().uuidString).sqlite")
    let store = RAGStore(dbURL: dbURL, embeddingProvider: StubEmbeddingProvider())
    try await store.openIfNeeded()
    try await store.ensureSchema()
    return store
  }

  /// A superproject shaped like the real one: mostly submodule content, plus a
  /// handful of files that genuinely belong to it.
  private func seedWorkspace(_ store: RAGStore) async throws {
    try await store.upsertRepo(
      id: "ws", name: "workspace", rootPath: "/ws",
      lastIndexedAt: nil, repoIdentifier: "github.com/x/ws"
    )
    let paths = [
      "api/app.rb",              // submodule content
      "api/lib/thing.rb",        // submodule content
      "front-end/index.js",      // submodule content
      "apiary/spec.md",          // sibling that merely SHARES A PREFIX with api
      "workspace/config.yml",    // genuinely the superproject's
      "README.md",               // genuinely the superproject's
    ]
    for (index, path) in paths.enumerated() {
      try await store.upsertFile(
        id: "f\(index)", repoId: "ws", path: path, hash: "h\(index)",
        language: "text", updatedAt: "2026-01-01", modulePath: nil, featureTags: nil
      )
      try await store.upsertChunk(
        id: "c\(index)", fileId: "f\(index)", startLine: 1, endLine: 2,
        text: "body \(index)", tokenCount: 3,
        constructType: nil, constructName: nil, metadata: nil
      )
    }
  }

  /// Read paths straight out of the table — RAGStore has no public per-repo
  /// file listing, and these tests need to assert on exact survivors.
  private func paths(_ store: RAGStore, repoId: String = "ws") async throws -> [String] {
    try await store.testOnlyFilePaths(repoId: repoId)
  }

  @Test("removes only the named subtrees")
  func removesOnlyNamedSubtrees() async throws {
    let store = try await makeStore()
    try await seedWorkspace(store)

    let removed = try await store.deleteFilesUnder(repoId: "ws", pathPrefixes: ["api", "front-end"])

    #expect(removed == 3)
    #expect(try await paths(store) == ["README.md", "apiary/spec.md", "workspace/config.yml"])
  }

  /// The whole point: the repo keeps existing, so it never disappears from the
  /// repo list and needs no re-index to come back.
  @Test("the repo row survives")
  func repoRowSurvives() async throws {
    let store = try await makeStore()
    try await seedWorkspace(store)

    _ = try await store.deleteFilesUnder(repoId: "ws", pathPrefixes: ["api", "front-end"])

    let repo = try await store.listRepos().first { $0.id == "ws" }
    #expect(repo != nil)
    #expect(repo?.rootPath == "/ws")
  }

  /// `apiary/` is not inside `api/`. Without the directory-boundary anchor a
  /// prefix eats its own siblings.
  @Test("a sibling sharing a prefix is untouched")
  func siblingSharingPrefixSurvives() async throws {
    let store = try await makeStore()
    try await seedWorkspace(store)

    _ = try await store.deleteFilesUnder(repoId: "ws", pathPrefixes: ["api"])

    #expect(try await paths(store).contains("apiary/spec.md"))
  }

  @Test("trailing slashes on the prefix change nothing")
  func trailingSlashIsNormalized() async throws {
    let store = try await makeStore()
    try await seedWorkspace(store)

    let removed = try await store.deleteFilesUnder(repoId: "ws", pathPrefixes: ["api/", "front-end///"])

    #expect(removed == 3)
    #expect(try await paths(store).contains("apiary/spec.md"))
  }

  @Test("chunks of deleted files go with them")
  func chunksAreCascaded() async throws {
    let store = try await makeStore()
    try await seedWorkspace(store)

    _ = try await store.deleteFilesUnder(repoId: "ws", pathPrefixes: ["api", "front-end"])

    // Anything still reachable must belong to a file that survived.
    #expect(try await paths(store).count == 3)
    let remainingChunks = try await store.testOnlyChunkCount(repoId: "ws")
    #expect(remainingChunks == 3, "one chunk per surviving file, none left behind")
  }

  @Test("an empty prefix list is a no-op")
  func emptyPrefixListDoesNothing() async throws {
    let store = try await makeStore()
    try await seedWorkspace(store)

    #expect(try await store.deleteFilesUnder(repoId: "ws", pathPrefixes: []) == 0)
    #expect(try await paths(store).count == 6)
  }

  /// A prefix of "" or "/" would normalize to empty and, unguarded, could widen
  /// into "delete everything".
  @Test("blank prefixes cannot widen into a full wipe")
  func blankPrefixesCannotWipe() async throws {
    let store = try await makeStore()
    try await seedWorkspace(store)

    #expect(try await store.deleteFilesUnder(repoId: "ws", pathPrefixes: ["", "/", "///"]) == 0)
    #expect(try await paths(store).count == 6)
  }

  @Test("a prefix matching nothing removes nothing")
  func unmatchedPrefixRemovesNothing() async throws {
    let store = try await makeStore()
    try await seedWorkspace(store)

    #expect(try await store.deleteFilesUnder(repoId: "ws", pathPrefixes: ["nope"]) == 0)
    #expect(try await paths(store).count == 6)
  }

  /// LIKE metacharacters in a real directory name must be literal.
  @Test("underscores and percents in a directory name are literal")
  func likeMetacharactersAreEscaped() async throws {
    let store = try await makeStore()
    try await store.upsertRepo(
      id: "ws", name: "workspace", rootPath: "/ws",
      lastIndexedAt: nil, repoIdentifier: "github.com/x/ws"
    )
    for (index, path) in ["a_b/one.txt", "axb/two.txt"].enumerated() {
      try await store.upsertFile(
        id: "f\(index)", repoId: "ws", path: path, hash: "h\(index)",
        language: "text", updatedAt: "2026-01-01", modulePath: nil, featureTags: nil
      )
    }

    let removed = try await store.deleteFilesUnder(repoId: "ws", pathPrefixes: ["a_b"])

    #expect(removed == 1, "'_' is a single-char wildcard in LIKE; it must not match 'axb'")
    #expect(try await paths(store) == ["axb/two.txt"])
  }

  @Test("another repo's identically-named subtree is untouched")
  func otherReposAreUntouched() async throws {
    let store = try await makeStore()
    try await seedWorkspace(store)
    try await store.upsertRepo(
      id: "other", name: "other", rootPath: "/other",
      lastIndexedAt: nil, repoIdentifier: "github.com/x/other"
    )
    try await store.upsertFile(
      id: "o0", repoId: "other", path: "api/app.rb", hash: "oh0",
      language: "text", updatedAt: "2026-01-01", modulePath: nil, featureTags: nil
    )

    _ = try await store.deleteFilesUnder(repoId: "ws", pathPrefixes: ["api"])

    #expect(try await paths(store, repoId: "other") == ["api/app.rb"])
  }
}
