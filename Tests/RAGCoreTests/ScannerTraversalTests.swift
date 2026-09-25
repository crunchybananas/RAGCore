@testable import RAGCore
import Foundation
import Testing

@Suite("File scanner traversal")
struct ScannerTraversalTests {
  @Test("Ignoring a lock file does not prune its later sibling directory")
  func ignoredFilePreservesSiblingDirectory() throws {
    let rootURL = FileManager.default.temporaryDirectory
      .appendingPathComponent("ragcore-scanner-traversal-\(UUID().uuidString)", isDirectory: true)
    defer { try? FileManager.default.removeItem(at: rootURL) }

    try FileManager.default.createDirectory(at: rootURL, withIntermediateDirectories: true)
    try "lock contents\n".write(
      to: rootURL.appendingPathComponent("poetry.lock"),
      atomically: true,
      encoding: .utf8
    )

    let packageURL = rootURL.appendingPathComponent("tas", isDirectory: true)
    try FileManager.default.createDirectory(at: packageURL, withIntermediateDirectories: true)
    let sourceURL = packageURL.appendingPathComponent("main.py")
    try "def main():\n    return 42\n".write(
      to: sourceURL,
      atomically: true,
      encoding: .utf8
    )

    let candidates = RAGFileScanner().scan(rootURL: rootURL)
    let paths = Set(candidates.map(\.path))

    #expect(paths.contains { $0.hasSuffix("/tas/main.py") })
    #expect(!paths.contains { $0.hasSuffix("/poetry.lock") })
  }

  @Test("Guidance directories are indexed; other hidden entries and in-repo worktrees are not")
  func guidanceDirectoriesAreIndexed() throws {
    let rootURL = FileManager.default.temporaryDirectory
      .appendingPathComponent("ragcore-scanner-hidden-\(UUID().uuidString)", isDirectory: true)
    defer { try? FileManager.default.removeItem(at: rootURL) }
    func write(_ relative: String, _ text: String = "# Notes\nUse the shared request helpers.\n") throws {
      let url = rootURL.appendingPathComponent(relative)
      try FileManager.default.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true)
      try text.write(to: url, atomically: true, encoding: .utf8)
    }
    try write(".github/instructions/testing.md")
    try write(".github/workflows/ci.yml", "name: ci\non: push\n")
    try write(".claude/skills/release/SKILL.md")
    try write(".claude/worktrees/copy/app/main.md")
    try write(".cursor/rules/style.md")
    try write(".worktrees/branch/app/other.md")
    try write(".vscode/notes.md")
    try write(".env", "API_TOKEN=secret\n")
    try write("app/worktrees/real.md")

    let outcome = RAGFileScanner().scanWithOutcome(rootURL: rootURL)
    // The enumerator reports /private/var/... for a /var/... temporary root, so
    // compare by suffix rather than by stripping the root prefix.
    let found = outcome.candidates.map(\.path)
    func contains(_ relative: String) -> Bool { found.contains { $0.hasSuffix("/" + relative) } }

    #expect(contains(".github/instructions/testing.md"))
    #expect(contains(".github/workflows/ci.yml"))
    #expect(contains(".claude/skills/release/SKILL.md"))
    #expect(contains(".cursor/rules/style.md"))
    #expect(contains("app/worktrees/real.md"), "only .claude/worktrees is excluded, not every worktrees directory")
    #expect(!contains(".claude/worktrees/copy/app/main.md"))
    #expect(!contains(".worktrees/branch/app/other.md"))
    #expect(!contains(".vscode/notes.md"))
    #expect(!contains(".env"))
    #expect(!outcome.policyExcludedPaths.contains { $0.hasSuffix(".env") }, "a hidden file is skipped before it is ever read or reported")
  }
}

