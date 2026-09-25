//
//  MarkdownSectionChunker.swift
//  RAGCore
//
//  Chunks Markdown by section instead of by line count.
//

import Foundation

/// Splits a Markdown document at its headings, so each chunk is one section
/// named by its heading path ("Data layer › Requests").
///
/// Line chunking cut a design document every ~120 lines regardless of
/// structure: a chunk started mid-section with nothing saying what it was
/// about, and a question about "requests" had to hope the word appeared in the
/// right slice. A section chunk carries its own title, and the title is
/// indexed as the construct name, which keyword search weighs.
public struct MarkdownSectionChunker: Sendable {
  /// Deepest heading level that starts a new chunk; deeper headings stay
  /// inside their parent section.
  public let splitLevel: Int
  /// A section longer than this is split into parts at blank lines.
  public let maxLines: Int
  /// A section with fewer non-empty body lines than this is folded into the
  /// section that follows it (a heading directly above a subheading).
  public let minBodyLines: Int

  public init(splitLevel: Int = 3, maxLines: Int = 120, minBodyLines: Int = 2) {
    self.splitLevel = max(1, splitLevel)
    self.maxLines = max(10, maxLines)
    self.minBodyLines = max(0, minBodyLines)
  }

  private struct Section {
    var path: [String]
    var start: Int  // 0-based line index, inclusive
    var end: Int  // 0-based line index, exclusive
  }

  public func chunk(text: String) -> [RAGChunk] {
    let lines = text.split(separator: "\n", omittingEmptySubsequences: false).map(String.init)
    guard lines.contains(where: { !$0.trimmingCharacters(in: .whitespaces).isEmpty }) else { return [] }

    // 1. Find section starts, ignoring headings inside fenced code blocks.
    var sections: [Section] = []
    var headingStack: [(level: Int, title: String)] = []
    var fence: String?
    var current = Section(path: [], start: 0, end: lines.count)
    for (index, line) in lines.enumerated() {
      let trimmed = line.trimmingCharacters(in: .whitespaces)
      if let open = fence {
        if trimmed.hasPrefix(open) { fence = nil }
        continue
      }
      if trimmed.hasPrefix("```") || trimmed.hasPrefix("~~~") {
        fence = String(trimmed.prefix(3))
        continue
      }
      guard let (level, title) = Self.heading(in: line), level <= splitLevel else { continue }
      if index > current.start {
        current.end = index
        sections.append(current)
      }
      headingStack.removeAll { $0.level >= level }
      headingStack.append((level, title))
      current = Section(path: headingStack.map(\.title), start: index, end: lines.count)
    }
    current.end = lines.count
    sections.append(current)

    // 2. Fold near-empty sections into the next one, keeping the deeper path.
    var folded: [Section] = []
    var pendingStart: Int?
    for (position, section) in sections.enumerated() {
      let bodyLines = lines[section.start..<section.end].dropFirst(section.path.isEmpty ? 0 : 1)
        .filter { !$0.trimmingCharacters(in: .whitespaces).isEmpty }.count
      let isLast = position == sections.count - 1
      if bodyLines < minBodyLines && !isLast {
        pendingStart = pendingStart ?? section.start
        continue
      }
      var merged = section
      if let start = pendingStart { merged.start = start; pendingStart = nil }
      folded.append(merged)
    }

    // 3. Emit, splitting long sections into parts at blank lines.
    var chunks: [RAGChunk] = []
    for section in folded {
      let name = section.path.isEmpty ? "Introduction" : section.path.joined(separator: " › ")
      let parts = split(range: section.start..<section.end, lines: lines)
      for (index, range) in parts.enumerated() {
        let body = lines[range].joined(separator: "\n")
        guard !body.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else { continue }
        chunks.append(RAGChunk(
          startLine: range.lowerBound + 1,
          endLine: range.upperBound,
          text: body,
          tokenCount: max(1, body.split { $0.isWhitespace || $0.isNewline }.count),
          constructType: "section",
          constructName: parts.count > 1 ? "\(name) (part \(index + 1)/\(parts.count))" : name
        ))
      }
    }
    return chunks
  }

  /// An ATX heading ("## Title", optional closing hashes), or nil.
  static func heading(in line: String) -> (Int, String)? {
    guard line.hasPrefix("#") else { return nil }
    let hashes = line.prefix { $0 == "#" }.count
    guard (1...6).contains(hashes) else { return nil }
    let rest = line.dropFirst(hashes)
    guard rest.first == " " || rest.first == "\t" else { return nil }
    var title = rest.trimmingCharacters(in: .whitespaces)
    while title.hasSuffix("#") { title.removeLast() }
    title = title.trimmingCharacters(in: .whitespaces)
    return title.isEmpty ? nil : (hashes, title)
  }

  /// Split a section into parts of at most `maxLines`, preferring blank lines.
  private func split(range: Range<Int>, lines: [String]) -> [Range<Int>] {
    guard range.count > maxLines else { return [range] }
    var parts: [Range<Int>] = []
    var start = range.lowerBound
    while start < range.upperBound {
      var end = min(range.upperBound, start + maxLines)
      if end < range.upperBound {
        let floor = start + maxLines / 2
        if let blank = (floor..<end).reversed().first(where: {
          lines[$0].trimmingCharacters(in: .whitespaces).isEmpty
        }) {
          end = blank + 1
        }
      }
      parts.append(start..<end)
      start = end
    }
    return parts
  }
}
