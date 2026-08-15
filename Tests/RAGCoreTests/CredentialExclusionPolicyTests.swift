//
//  CredentialExclusionPolicyTests.swift
//  RAGCoreTests
//
//  Pins the mandatory credential boundary (#11): credential-shaped files are
//  never read or stored, .ragignore cannot weaken the policy, the opt-in is
//  per named pattern, root .gitignore patterns merge as excludes, and the
//  purge removes every row class an already-indexed credential left behind.
//

@testable import RAGCore
import Foundation
import Testing

private struct StubEmbeddingProvider: EmbeddingProvider {
  var dimensions: Int { 3 }
  var modelName: String { "stub" }
  func embed(texts: [String]) async throws -> [[Float]] { texts.map { _ in [0, 0, 0] } }
}

// MARK: - Policy unit

@Suite("CredentialExclusionPolicy")
struct CredentialExclusionPolicyUnitTests {

  private func excludes(_ path: String, policy: CredentialExclusionPolicy = .standard) -> Bool {
    policy.excludes(relativePath: path, fileName: (path as NSString).lastPathComponent)
  }

  @Test("keys and signing material are refused")
  func keysRefused() {
    #expect(excludes("deploy/server.pem"))
    #expect(excludes("certs/tls.key"))
    #expect(excludes("ios/dist.p12"))
    #expect(excludes("android/release.keystore"))
    #expect(excludes("ssh/id_rsa"))
    #expect(excludes("ssh/id_ed25519.pub"))
    #expect(excludes("profiles/app.mobileprovision"))
    #expect(excludes("profiles/mac.provisionprofile"))
  }

  @Test(".env variants are refused, including the non-hidden ones")
  func envRefused() {
    #expect(excludes(".env"))
    #expect(excludes(".env.local"))
    // The `env` extension is in the scanner allowlist, so these were READ
    // and INDEXED before the policy existed — the exact hole #11 names.
    #expect(excludes("config/prod.env"))
    #expect(excludes("staging.env"))
  }

  @Test("service accounts, OAuth, and platform bundles are refused")
  func serviceAccountsRefused() {
    #expect(excludes("gcp/my-project-service-account.json"))
    #expect(excludes("gcp/svc_service_account_key.json"))
    #expect(excludes("oauth/client_secret_1234.apps.googleusercontent.com.json"))
    #expect(excludes("credentials.json"))
    #expect(excludes("app/google-services.json"))
    #expect(excludes("ios/GoogleService-Info.plist"))
    #expect(excludes("infra/prod.tfvars"))
    #expect(excludes("secrets.yaml"))
  }

  @Test("ordinary source and config files pass")
  func ordinaryFilesPass() {
    #expect(!excludes("Sources/App/main.swift"))
    #expect(!excludes("package.json"))
    #expect(!excludes("Environment.swift"))
    #expect(!excludes("config/settings.yaml"))
    #expect(!excludes("Tests/KeychainTests.swift"))
  }

  @Test("the unsafe opt-in is per named pattern, never a blanket disable")
  func optInIsPerPattern() {
    let policy = CredentialExclusionPolicy(unsafeAllowPatterns: ["*.pem"])
    #expect(!excludes("fixtures/test.pem", policy: policy), "the named pattern is re-allowed")
    #expect(excludes("certs/tls.key", policy: policy), "every other pattern still applies")
    #expect(excludes("ssh/id_rsa", policy: policy))
    // A free-form allow that names no mandatory pattern changes nothing.
    let noop = CredentialExclusionPolicy(unsafeAllowPatterns: ["*.harmless"])
    #expect(excludes("deploy/server.pem", policy: noop))
  }
}

// MARK: - Scanner integration

@Suite("Scanner credential boundary")
struct ScannerCredentialBoundaryTests {

  private func makeRepo(files: [(path: String, contents: String)]) throws -> URL {
    let root = FileManager.default.temporaryDirectory
      .appendingPathComponent("ragcore-cred-scan-\(UUID().uuidString)")
    for file in files {
      let url = root.appendingPathComponent(file.path)
      try FileManager.default.createDirectory(
        at: url.deletingLastPathComponent(), withIntermediateDirectories: true)
      try file.contents.write(to: url, atomically: true, encoding: .utf8)
    }
    return root
  }

  @Test("credential files are refused before read and reported by path")
  func refusedAndReported() throws {
    let root = try makeRepo(files: [
      ("Sources/main.swift", "print(1)"),
      ("config/prod.env", "SECRET=x"),
      ("gcp/service-account-key.json", "{}"),
    ])
    defer { try? FileManager.default.removeItem(at: root) }

    let outcome = RAGFileScanner().scanWithOutcome(rootURL: root)
    let candidatePaths = outcome.candidates.map(\.path)
    #expect(candidatePaths.contains { $0.hasSuffix("Sources/main.swift") })
    #expect(!candidatePaths.contains { $0.hasSuffix("prod.env") })
    #expect(!candidatePaths.contains { $0.hasSuffix("service-account-key.json") })
    #expect(Set(outcome.policyExcludedPaths) == ["config/prod.env", "gcp/service-account-key.json"])
  }

  @Test(".ragignore content cannot re-include a credential file")
  func ragignoreCannotWeaken() throws {
    let root = try makeRepo(files: [
      (".ragignore", "!*.env\n!prod.env\n"),
      ("config/prod.env", "SECRET=x"),
    ])
    defer { try? FileManager.default.removeItem(at: root) }

    let outcome = RAGFileScanner().scanWithOutcome(rootURL: root)
    #expect(outcome.candidates.isEmpty)
    #expect(outcome.policyExcludedPaths == ["config/prod.env"])
  }

  @Test("a symlink named innocently still refuses its credential target")
  func symlinkTargetIsChecked() throws {
    let root = try makeRepo(files: [
      ("ssh/id_rsa", "PRIVATE KEY"),
      ("Sources/main.swift", "print(1)"),
    ])
    defer { try? FileManager.default.removeItem(at: root) }
    // The link has an allowlisted extension and an innocent name.
    let link = root.appendingPathComponent("notes.md")
    try FileManager.default.createSymbolicLink(
      at: link, withDestinationURL: root.appendingPathComponent("ssh/id_rsa"))

    let outcome = RAGFileScanner().scanWithOutcome(rootURL: root)
    #expect(!outcome.candidates.contains { $0.path.hasSuffix("notes.md") })
    #expect(outcome.policyExcludedPaths.contains("notes.md"))
  }

  @Test("root .gitignore patterns merge as excludes; negations are dropped")
  func gitignoreMerges() throws {
    let root = try makeRepo(files: [
      (".gitignore", "generated/\n*.log\n!keep.log\n"),
      ("generated/out.swift", "print(1)"),
      ("Sources/main.swift", "print(1)"),
    ])
    defer { try? FileManager.default.removeItem(at: root) }

    let outcome = RAGFileScanner().scanWithOutcome(rootURL: root)
    let paths = outcome.candidates.map(\.path)
    #expect(paths.contains { $0.hasSuffix("Sources/main.swift") })
    #expect(!paths.contains { $0.hasSuffix("generated/out.swift") },
            "root .gitignore directory pattern excludes at scan")
  }

  @Test("loadFile refuses a credential candidate on its own")
  func loadFileIsDefenseInDepth() throws {
    let root = try makeRepo(files: [("gcp/credentials.json", "{}")])
    defer { try? FileManager.default.removeItem(at: root) }
    let candidate = RAGFileCandidate(
      path: root.appendingPathComponent("gcp/credentials.json").path,
      byteCount: 2, language: "json")
    #expect(RAGFileScanner().loadFile(candidate: candidate) == nil)
  }
}

// MARK: - Audit + purge

@Suite("Policy audit and purge")
struct PolicyPurgeTests {

  private func makeStore() async throws -> RAGStore {
    let dbURL = FileManager.default.temporaryDirectory
      .appendingPathComponent("ragcore-cred-purge-\(UUID().uuidString).sqlite")
    let store = RAGStore(dbURL: dbURL, embeddingProvider: StubEmbeddingProvider())
    try await store.openIfNeeded()
    try await store.ensureSchema()
    return store
  }

  /// A corpus indexed BEFORE the policy existed: one credential file and one
  /// ordinary file, each with a chunk, an embedding, and an AI analysis row.
  private func seedLegacyCorpus(_ store: RAGStore) async throws {
    try await store.upsertRepo(
      id: "r", name: "repo", rootPath: "/r",
      lastIndexedAt: nil, repoIdentifier: "github.com/x/r")
    for (fileId, path) in [("f-cred", "config/prod.env"), ("f-ok", "Sources/main.swift")] {
      try await store.upsertFile(
        id: fileId, repoId: "r", path: path, hash: "h",
        language: "swift", updatedAt: "2026-01-01", modulePath: nil, featureTags: nil)
      try await store.upsertChunk(
        id: "c-\(fileId)", fileId: fileId, startLine: 1, endLine: 2,
        text: "body", tokenCount: 2,
        constructType: nil, constructName: nil, metadata: nil)
      try await store.upsertEmbedding(chunkId: "c-\(fileId)", vector: [0, 0, 0])
      try await store.updateChunkAnalysis(
        chunkId: "c-\(fileId)", chunkText: "body", aiSummary: "s", aiTags: nil,
        analyzedAt: "2026-01-01", analyzerModel: "stub")
    }
  }

  @Test("audit names the credential paths without touching contents")
  func auditNamesPaths() async throws {
    let store = try await makeStore()
    try await seedLegacyCorpus(store)
    let audited = try await store.auditPolicyExclusions(repoId: "r")
    #expect(audited == ["config/prod.env"])
  }

  @Test("purge removes every row class and leaves the rest intact")
  func purgeRemovesEverything() async throws {
    let store = try await makeStore()
    try await seedLegacyCorpus(store)

    let report = try await store.purgePolicyExclusions(repoId: "r")
    #expect(report.purgedPaths == ["config/prod.env"])
    #expect(report.chunksRemoved == 1)

    // The credential file's rows are gone — including chunk_analysis, which
    // every pre-existing delete path left behind.
    #expect(try await store.testOnlyRowCount(table: "files", whereClause: "id = 'f-cred'") == 0)
    #expect(try await store.testOnlyRowCount(table: "chunks", whereClause: "file_id = 'f-cred'") == 0)
    #expect(try await store.testOnlyRowCount(table: "embeddings", whereClause: "chunk_id = 'c-f-cred'") == 0)
    #expect(try await store.testOnlyRowCount(table: "chunk_analysis", whereClause: "chunk_id = 'c-f-cred'") == 0)
    // The ordinary file survives untouched.
    #expect(try await store.testOnlyRowCount(table: "files", whereClause: "id = 'f-ok'") == 1)
    #expect(try await store.testOnlyRowCount(table: "chunk_analysis", whereClause: "chunk_id = 'c-f-ok'") == 1)
    #expect(try await store.integrityReport().isClean)
    // Idempotent: a second purge finds nothing.
    let again = try await store.purgePolicyExclusions(repoId: "r")
    #expect(again.purgedPaths.isEmpty)
  }
}
