//
//  RAGLineChunker.swift
//  RAGCore
//
//  Line-based chunker with semantic boundary detection.
//

import Foundation

/// Line-based text chunker that splits source code into semantically meaningful chunks.
///
/// Splits text by line count, preferring natural boundaries like function
/// definitions, class declarations, and section markers to avoid breaking
/// code mid-construct.
public struct RAGLineChunker: Sendable {
  public let maxLines: Int
  public let minLines: Int
  public let overlapLines: Int

  public init(maxLines: Int = 120, minLines: Int = 20, overlapLines: Int = 5) {
    self.maxLines = maxLines
    self.minLines = minLines
    self.overlapLines = overlapLines
  }

  public func chunk(text: String) -> [RAGChunk] {
    let lines = text.split(separator: "\n", omittingEmptySubsequences: false).map(String.init)
    guard !lines.isEmpty else { return [] }

    var chunks: [RAGChunk] = []
    chunks.reserveCapacity(max(1, lines.count / maxLines))

    var start = 0
    while start < lines.count {
      var end = min(lines.count, start + maxLines)

      if end < lines.count {
        end = findSemanticBoundary(lines: lines, from: start, preferredEnd: end)
      }

      let slice = lines[start..<end]
      let chunkText = slice.joined(separator: "\n")
      let tokenCount = approximateTokenCount(for: chunkText)
      chunks.append(
        RAGChunk(
          startLine: start + 1,
          endLine: end,
          text: chunkText,
          tokenCount: tokenCount
        )
      )
      if end == lines.count { break }
      start = max(0, end - overlapLines)
    }

    return chunks
  }

  /// Find a semantic boundary (MARK comment, type definition, extension, etc.)
  /// Search backwards from preferredEnd to find a good split point.
  private func findSemanticBoundary(lines: [String], from start: Int, preferredEnd: Int) -> Int {
    let boundaryPatterns = [
      "// MARK: -",
      "// MARK:",
      "// FIXME:",
      "// TODO:",
      "// ===",
      "// ---",
      "struct ",
      "class ",
      "enum ",
      "protocol ",
      "extension ",
      "actor ",
      "func ",
      "public func ",
      "private func ",
      "internal func ",
      "@MainActor",
      "@Observable",
      "## ",
      "### ",
      "#### ",
    ]

    let searchStart = max(start + minLines, preferredEnd - 30)

    for i in stride(from: preferredEnd - 1, through: searchStart, by: -1) {
      let trimmed = lines[i].trimmingCharacters(in: .whitespaces)

      for pattern in boundaryPatterns {
        if trimmed.hasPrefix(pattern) {
          return i
        }
      }

      if trimmed.isEmpty && i + 1 < lines.count {
        let nextTrimmed = lines[i + 1].trimmingCharacters(in: .whitespaces)
        if nextTrimmed.hasPrefix("//") || nextTrimmed.hasPrefix("///") ||
            nextTrimmed.hasPrefix("struct") || nextTrimmed.hasPrefix("class") ||
            nextTrimmed.hasPrefix("func") || nextTrimmed.hasPrefix("enum") ||
            nextTrimmed.hasPrefix("extension") || nextTrimmed.hasPrefix("protocol") {
          return i + 1
        }
      }
    }

    return preferredEnd
  }

  private func approximateTokenCount(for text: String) -> Int {
    let words = text.split { $0.isWhitespace || $0.isNewline }
    return max(1, words.count)
  }
}
