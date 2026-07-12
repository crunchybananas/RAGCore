@testable import RAGCore
import Foundation
import Testing

/// Regression tests for ignore-aware workspace/sub-package detection (#4).
///
/// `detectWorkspaceRepos` used a hardcoded exclusion list that missed SPM
/// build output: an alternate derived-data directory inside an app repo
/// (e.g. `build-ios-check/SourcePackages/checkouts/*`) contributed dozens of
/// vendored dependency checkouts, flipping `indexRepository` into its
/// workspace-split branch which registered and indexed every one of them.
/// Detection now prunes `SourcePackages`/`Pods`/`*.noindex` and honors the
/// scan root's `.ragignore` — the same file the file scanner reads.
private struct StubEmbeddingProvider: EmbeddingProvider {
  var dimensions: Int { 3 }
  var modelName: String { "stub" }
  func embed(texts: [String]) async throws -> [[Float]] { texts.map { _ in [0, 0, 0] } }
}

@Suite("workspace detection ignores build output + .ragignore (#4)")
struct WorkspaceDetectionTests {

  private func makeStore() async throws -> RAGStore {
    let dbURL = FileManager.default.temporaryDirectory
      .appendingPathComponent("ragcore-wsdetect-\(UUID().uuidString).sqlite")
    let store = RAGStore(dbURL: dbURL, embeddingProvider: StubEmbeddingProvider())
    try await store.openIfNeeded()
    try await store.ensureSchema()
    return store
  }

  /// Fresh temp root; caller-built trees live under it.
  private func makeRoot() throws -> URL {
    let root = FileManager.default.temporaryDirectory
      .appendingPathComponent("ragcore-ws-\(UUID().uuidString)", isDirectory: true)
    try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
    return root
  }

  /// Create a fake git repo (a directory containing `.git/`) at a relative path.
  private func addGitRepo(_ root: URL, _ relative: String) throws {
    let repo = root.appendingPathComponent(relative, isDirectory: true)
    try FileManager.default.createDirectory(
      at: repo.appendingPathComponent(".git", isDirectory: true),
      withIntermediateDirectories: true)
  }

  private func write(_ root: URL, _ relative: String, _ contents: String) throws {
    let url = root.appendingPathComponent(relative)
    try FileManager.default.createDirectory(
      at: url.deletingLastPathComponent(), withIntermediateDirectories: true)
    try contents.write(to: url, atomically: true, encoding: .utf8)
  }

  @Test("plain multi-repo workspace is still detected")
  func plainWorkspaceDetected() async throws {
    let store = try await makeStore()
    let root = try makeRoot()
    defer { try? FileManager.default.removeItem(at: root) }
    try addGitRepo(root, "alpha")
    try addGitRepo(root, "beta")

    let repos = await store.detectWorkspaceRepos(rootURL: root)
    #expect(repos.map { URL(fileURLWithPath: $0).lastPathComponent }.sorted() == ["alpha", "beta"])
  }

  @Test("SPM SourcePackages, Pods, and *.noindex never contribute workspace members")
  func buildOutputPruned() async throws {
    let store = try await makeStore()
    let root = try makeRoot()
    defer { try? FileManager.default.removeItem(at: root) }
    try addGitRepo(root, "real-repo")
    // SPM derived-data content: one git repo per dependency.
    try addGitRepo(root, "some-derived/SourcePackages/checkouts/dep-a")
    try addGitRepo(root, "some-derived/SourcePackages/repositories/dep-b-1a2b3c")
    try addGitRepo(root, "Pods/VendoredPod")
    try addGitRepo(root, "Cache.noindex/dep-c")

    let repos = await store.detectWorkspaceRepos(rootURL: root)
    #expect(repos.map { URL(fileURLWithPath: $0).lastPathComponent } == ["real-repo"])
  }

  @Test("root .ragignore directory patterns prune discovery")
  func ragignorePrunesDiscovery() async throws {
    let store = try await makeStore()
    let root = try makeRoot()
    defer { try? FileManager.default.removeItem(at: root) }
    try write(root, ".ragignore", "# build output\nbuild-ios-check/\n**/generated-vendor/\n")
    try addGitRepo(root, "real-repo")
    try addGitRepo(root, "build-ios-check/vendored-dep")
    try addGitRepo(root, "nested/generated-vendor/dep")
    // gitignore semantics: `**/x/` matches x at ANY depth, including root.
    try addGitRepo(root, "generated-vendor/root-level-dep")

    let repos = await store.detectWorkspaceRepos(rootURL: root)
    #expect(repos.map { URL(fileURLWithPath: $0).lastPathComponent } == ["real-repo"])
  }

  @Test("a non-matching .ragignore does not over-prune")
  func ragignoreDoesNotOverPrune() async throws {
    let store = try await makeStore()
    let root = try makeRoot()
    defer { try? FileManager.default.removeItem(at: root) }
    try write(root, ".ragignore", "*.log\nsecrets/\n")
    try addGitRepo(root, "alpha")
    try addGitRepo(root, "beta")

    let repos = await store.detectWorkspaceRepos(rootURL: root)
    #expect(repos.count == 2)
  }

  @Test("sub-package detection honors the same pruning")
  func subPackagesPruned() async throws {
    let store = try await makeStore()
    let root = try makeRoot()
    defer { try? FileManager.default.removeItem(at: root) }
    try write(root, ".ragignore", "ignored-dir/\n")
    try write(root, "apps/web/package.json", "{}")
    try write(root, "ignored-dir/pkg/package.json", "{}")
    try write(root, "other-derived/SourcePackages/checkouts/dep/Package.swift", "// swift-tools-version:6.0")

    let packages = await store.detectSubPackages(rootURL: root, excludingGitRepos: [])
    #expect(packages.map { URL(fileURLWithPath: $0).lastPathComponent } == ["web"])
  }

  @Test("file patterns in .ragignore do not swallow manifest FILES")
  func filePatternsDoNotSuppressManifests() async throws {
    // `*.json` (or `*.toml`, …) in the root .ragignore is a file-CONTENT
    // exclusion; it must not delete the package from index topology by
    // hiding its manifest from detection. Directory-mode matching only
    // applies to directories.
    let store = try await makeStore()
    let root = try makeRoot()
    defer { try? FileManager.default.removeItem(at: root) }
    try write(root, ".ragignore", "*.json\n*.toml\n")
    try write(root, "apps/web/package.json", "{}")
    try write(root, "services/agent/Cargo.toml", "[package]")

    let packages = await store.detectSubPackages(rootURL: root, excludingGitRepos: [])
    #expect(packages.map { URL(fileURLWithPath: $0).lastPathComponent }.sorted() == ["agent", "web"])
  }
}
