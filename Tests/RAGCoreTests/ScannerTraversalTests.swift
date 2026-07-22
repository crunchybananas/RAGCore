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
}
