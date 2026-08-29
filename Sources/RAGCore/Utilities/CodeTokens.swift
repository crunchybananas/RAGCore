//
//  CodeTokens.swift
//  RAGCore
//
//  Code-aware tokenization for the FTS5 lexical index (cloke/peel#2211).
//
//  FTS5's unicode61 tokenizer already splits snake_case and dotted paths at
//  the punctuation, but it cannot see inside camelCase: `resolveRepo` is one
//  token to it, invisible to a query for `resolve` or `repo`. These helpers
//  close that gap without a custom C tokenizer: at index time the original
//  text is emitted unchanged (so exact-identifier queries keep working) and
//  every multi-hump identifier's parts are appended adjacently (so part
//  queries and part *phrases* match); at query time each word expands to the
//  same variants.
//

import Foundation

public enum CodeTokens {

  // MARK: - Index-time transform

  /// The string the FTS index should tokenize for a piece of source text:
  /// the original text, followed by the camelCase splits of every identifier
  /// that has any. Parts are emitted adjacently, in order, so a phrase query
  /// built from the same split matches them.
  ///
  /// Registered with SQLite as the `code_tokens(x)` scalar function; the
  /// chunk triggers and the backfill statement both go through it, so the
  /// index can never disagree with the transform.
  public static func indexText(_ text: String) -> String {
    guard !text.isEmpty else { return text }
    var appended: [String] = []
    forEachIdentifier(in: text) { identifier in
      let parts = camelParts(identifier)
      if parts.count > 1 {
        appended.append(parts.joined(separator: " "))
      }
    }
    guard !appended.isEmpty else { return text }
    return text + " " + appended.joined(separator: " ")
  }

  // MARK: - Query-time transform

  /// Build an FTS5 MATCH expression for a user query. Each word becomes a
  /// quoted term; a camelCase word additionally matches the phrase of its
  /// parts, so `resolveRepo` finds both the exact identifier and the split
  /// form the index appended. Words join with AND (`matchAll`) or OR.
  /// Returns nil when the query has no usable terms.
  public static func matchExpression(for query: String, matchAll: Bool) -> String? {
    let words = query
      .components(separatedBy: .whitespacesAndNewlines)
      .filter { !$0.isEmpty }
    let clauses = words.compactMap { wordClause($0) }
    guard !clauses.isEmpty else { return nil }
    return clauses.joined(separator: matchAll ? " AND " : " OR ")
  }

  private static func wordClause(_ word: String) -> String? {
    // Quoting makes every term a string literal to FTS5, so operator
    // characters in the query (-, ^, *, NEAR) cannot change the query shape.
    // unicode61 then re-tokenizes the literal, which turns a snake_case or
    // dotted word into an implicit phrase of its parts - the same thing it
    // did to that word at index time.
    let sanitized = word.replacingOccurrences(of: "\"", with: "")
    guard sanitized.contains(where: { $0.isLetter || $0.isNumber }) else { return nil }
    var variants = [quoted(sanitized)]
    let parts = camelParts(sanitized)
    if parts.count > 1 {
      variants.append(quoted(parts.joined(separator: " ")))
    }
    return variants.count == 1 ? variants[0] : "(" + variants.joined(separator: " OR ") + ")"
  }

  private static func quoted(_ term: String) -> String {
    "\"" + term + "\""
  }

  // MARK: - Splitting

  /// Split one identifier at camelCase hump boundaries, treating an
  /// UPPER-UPPER-lower run as an acronym followed by a word (`HTTPServer` →
  /// `http server`). Returns lowercase parts; a single-part identifier
  /// returns itself lowercased.
  static func camelParts(_ identifier: String) -> [String] {
    var parts: [String] = []
    var current = ""
    let scalars = Array(identifier.unicodeScalars)
    for index in scalars.indices {
      let scalar = scalars[index]
      guard let character = Character(String(scalar)) as Character? else { continue }
      if character.isUppercase, !current.isEmpty {
        let previous = scalars[index - 1]
        let previousIsLower = Character(String(previous)).isLowercase
        let nextIsLower = index + 1 < scalars.count
          && Character(String(scalars[index + 1])).isLowercase
        if previousIsLower || (Character(String(previous)).isUppercase && nextIsLower) {
          parts.append(current.lowercased())
          current = ""
        }
      }
      current.unicodeScalars.append(scalar)
    }
    if !current.isEmpty {
      parts.append(current.lowercased())
    }
    return parts
  }

  /// Walk `text` and yield each identifier-shaped run (letters, digits,
  /// underscores) that contains at least one letter.
  private static func forEachIdentifier(in text: String, _ body: (String) -> Void) {
    var current = ""
    var sawLetter = false
    var sawUpperAfterFirst = false
    func flush() {
      // Only identifiers with an interior uppercase can have camel humps;
      // skipping the rest keeps the appended stream proportional to the
      // camelCase density, not the text size.
      if sawLetter, sawUpperAfterFirst, current.count > 2 {
        body(current)
      }
      current = ""
      sawLetter = false
      sawUpperAfterFirst = false
    }
    for character in text {
      if character.isLetter || character.isNumber || character == "_" {
        if character.isLetter { sawLetter = true }
        if character.isUppercase, !current.isEmpty { sawUpperAfterFirst = true }
        current.append(character)
      } else {
        flush()
      }
    }
    flush()
  }
}
