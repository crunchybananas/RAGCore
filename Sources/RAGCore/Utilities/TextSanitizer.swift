//
//  TextSanitizer.swift
//  RAGCore
//
//  Text sanitization utilities for safe embedding input.
//

import Foundation

/// Text sanitization utilities to prevent NLEmbedding crashes from malformed input.
public enum TextSanitizer {

  /// Maximum text length to prevent CoreNLP crashes.
  private static let maxTextLength = 10_000

  /// Sanitizes text to prevent NLEmbedding/CoreNLP crashes from malformed input.
  ///
  /// - Truncates to 10,000 characters
  /// - Removes null bytes and control characters
  /// - Keeps printable ASCII, extended Unicode, newlines, tabs, whitespace
  /// - Collapses excessive whitespace
  /// - Trims leading/trailing whitespace
  ///
  /// - Parameter text: Input text to sanitize.
  /// - Returns: Sanitized text safe for embedding, or empty string if input is empty/whitespace-only.
  public static func sanitize(_ text: String) -> String {
    var result = text.count > maxTextLength ? String(text.prefix(maxTextLength)) : text

    result = result.unicodeScalars
      .filter { scalar in
        scalar == "\n" || scalar == "\r" || scalar == "\t" ||
          scalar.properties.isWhitespace ||
          (scalar.value >= 0x20 && scalar.value < 0x7F) ||
          (scalar.value >= 0xA0 && !scalar.properties.isNoncharacterCodePoint)
      }
      .map { Character($0) }
      .reduce(into: "") { $0.append($1) }

    result =
      result
      .components(separatedBy: .whitespacesAndNewlines)
      .filter { !$0.isEmpty }
      .joined(separator: " ")
      .trimmingCharacters(in: .whitespacesAndNewlines)

    return result
  }
}
