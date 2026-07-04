//
//  RepoIdentityTests.swift
//  RAGCoreTests
//
//  Canonical, never-empty repo identity (cloke/peel#1509). Pins the identity
//  precedence (remote URL → commit://<first-commit-hash> → local://<path>) and
//  the upgrade/no-downgrade rules that keep a re-index from blanking or
//  weakening a good identifier.
//

@testable import RAGCore
import Testing
import Foundation

@Suite("Repo canonical identity (#1509)")
struct RepoIdentityTests {

  // MARK: - Pure strength / preference logic

  @Test("identifier strength orders remote > commit:// > local://")
  func strengthOrdering() {
    #expect(RAGStore.identifierStrength("github.com/cloke/peel") == 2)
    #expect(RAGStore.identifierStrength("commit://abc123") == 1)
    #expect(RAGStore.identifierStrength("local:///Users/x/repo") == 0)
    #expect(RAGStore.identifierStrength("github.com/a/b") > RAGStore.identifierStrength("commit://h"))
    #expect(RAGStore.identifierStrength("commit://h") > RAGStore.identifierStrength("local://p"))
  }

  @Test("preferredIdentifier fills a nil/empty existing value")
  func preferFillsEmpty() {
    #expect(RAGStore.preferredIdentifier(existing: nil, discovered: "commit://h") == "commit://h")
    #expect(RAGStore.preferredIdentifier(existing: "", discovered: "github.com/a/b") == "github.com/a/b")
    #expect(RAGStore.preferredIdentifier(existing: nil, discovered: "local:///p") == "local:///p")
  }

  @Test("preferredIdentifier never downgrades a stronger stored identity")
  func preferNoDowngrade() {
    // A transient git/remote failure degrades discovery to commit://<hash> or
    // local:// — the good stored remote URL must survive the re-index.
    #expect(RAGStore.preferredIdentifier(existing: "github.com/a/b", discovered: "commit://h") == "github.com/a/b")
    #expect(RAGStore.preferredIdentifier(existing: "github.com/a/b", discovered: "local:///p") == "github.com/a/b")
    #expect(RAGStore.preferredIdentifier(existing: "commit://h", discovered: "local:///p") == "commit://h")
  }

  @Test("preferredIdentifier upgrades a weaker stored identity")
  func preferUpgrade() {
    // A repo first seen as a bare dir (local://) that later becomes a git repo,
    // or a no-remote repo that gains a remote, upgrades to the stronger identity.
    #expect(RAGStore.preferredIdentifier(existing: "local:///p", discovered: "commit://h") == "commit://h")
    #expect(RAGStore.preferredIdentifier(existing: "local:///p", discovered: "github.com/a/b") == "github.com/a/b")
    #expect(RAGStore.preferredIdentifier(existing: "commit://h", discovered: "github.com/a/b") == "github.com/a/b")
  }

  @Test("preferredIdentifier keeps the existing value on an equal-strength tie (no churn)")
  func preferTieKeepsExisting() {
    #expect(RAGStore.preferredIdentifier(existing: "github.com/a/b", discovered: "github.com/c/d") == "github.com/a/b")
    #expect(RAGStore.preferredIdentifier(existing: "commit://h1", discovered: "commit://h2") == "commit://h1")
  }

  // MARK: - Discovery against a real git working tree

  @Test("no-remote repo → commit://<first-commit-hash>, never empty")
  func canonicalNoRemote() throws {
    let dir = try makeTempGitRepo(remote: nil)
    defer { try? FileManager.default.removeItem(atPath: dir) }
    let hash = try #require(RAGStore.discoverFirstCommitHash(for: dir))
    #expect(!hash.isEmpty)
    #expect(RAGStore.discoverCanonicalIdentifier(for: dir) == "commit://\(hash)")
  }

  @Test("repo with a remote → normalized remote URL (preferred over the hash)")
  func canonicalWithRemote() throws {
    let dir = try makeTempGitRepo(remote: "git@github.com:cloke/peel.git")
    defer { try? FileManager.default.removeItem(atPath: dir) }
    #expect(RAGStore.discoverCanonicalIdentifier(for: dir) == "github.com/cloke/peel")
  }

  @Test("non-git directory → local://<path>, never empty")
  func canonicalNonGit() throws {
    let dir = NSTemporaryDirectory() + "ragcore-nongit-" + UUID().uuidString
    try FileManager.default.createDirectory(atPath: dir, withIntermediateDirectories: true)
    defer { try? FileManager.default.removeItem(atPath: dir) }
    #expect(RAGStore.discoverFirstCommitHash(for: dir) == nil)
    #expect(RAGStore.discoverNormalizedRemoteURL(for: dir) == nil)
    #expect(RAGStore.discoverCanonicalIdentifier(for: dir) == "local://\(dir)")
  }

  // MARK: - Helpers

  private func makeTempGitRepo(remote: String?) throws -> String {
    let dir = NSTemporaryDirectory() + "ragcore-git-" + UUID().uuidString
    try FileManager.default.createDirectory(atPath: dir, withIntermediateDirectories: true)
    git("init", "-q", in: dir)
    git("config", "user.email", "test@example.com", in: dir)
    git("config", "user.name", "Test", in: dir)
    git("config", "commit.gpgsign", "false", in: dir)
    FileManager.default.createFile(atPath: dir + "/README.md", contents: Data("hi".utf8))
    git("add", ".", in: dir)
    git("commit", "-q", "-m", "init", in: dir)
    if let remote {
      git("remote", "add", "origin", remote, in: dir)
    }
    return dir
  }

  @discardableResult
  private func git(_ args: String..., in dir: String) -> Int32 {
    let process = Process()
    process.executableURL = URL(fileURLWithPath: "/usr/bin/git")
    process.arguments = args
    process.currentDirectoryURL = URL(fileURLWithPath: dir)
    process.standardOutput = FileHandle.nullDevice
    process.standardError = FileHandle.nullDevice
    try? process.run()
    process.waitUntilExit()
    return process.terminationStatus
  }
}
