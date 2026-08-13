//
//  ChunkStructureMetrics.swift
//  RAGCore
//
//  Structural properties of a chunk, computed at index time.
//

import Foundation

/// Comment-shape metrics for one chunk.
///
/// These exist because a whole class of question is structural, not semantic,
/// and retrieval cannot express it: "files with a high ratio of comment lines to
/// code", "comments containing commented-out code", "comment blocks over N
/// lines". None of those has a phrasing that reliably retrieves it. A vector
/// query for "commented-out code left in place" ranks prose *about* cleanup
/// above code that exhibits it, and a keyword query has nothing to match. In the
/// session that motivated this, `grep -c` beat the index on all three
/// (cloke/peel#2199, #2202).
///
/// They are cheap — one pass over lines the chunker already holds — and they
/// turn "find me cleanup candidates" into a real query.
///
/// `containsCommentedOutCode` is a heuristic, so it was measured rather than
/// eyeballed. Against 684 real Swift files: 17 flagged, a 2.4% false-positive
/// rate, and the survivors are genuine code-in-comments (usage examples in doc
/// comments). Against the same corpus with a run of four real code lines
/// commented out in each file: 97% detected. Every tightening below came from
/// that measurement, not from taste — the first version flagged 20% of files,
/// almost all of them ordinary sentences.
///
/// The flag means "this chunk has comment lines that are code, not prose". A
/// usage example in a doc comment is a true positive by that definition even
/// though it is not cleanup work; callers filter.
public struct ChunkStructureMetrics: Sendable, Equatable {
  /// Lines that are entirely comment (or inside a block comment).
  public let commentLines: Int
  /// Lines carrying code. Blank lines count as neither.
  public let codeLines: Int
  /// Longest run of consecutive comment lines.
  public let maxCommentBlockLines: Int
  /// Whether any comment body looks like commented-out code.
  public let containsCommentedOutCode: Bool

  /// Comment lines as a fraction of non-blank lines, or nil when the chunk has
  /// no non-blank lines to take a ratio of.
  ///
  /// Deliberately derived rather than stored: persisting a rounded ratio
  /// alongside its own inputs invites the two disagreeing after a backfill.
  public var commentLineRatio: Double? {
    let total = commentLines + codeLines
    guard total > 0 else { return nil }
    return Double(commentLines) / Double(total)
  }

  public init(
    commentLines: Int,
    codeLines: Int,
    maxCommentBlockLines: Int,
    containsCommentedOutCode: Bool
  ) {
    self.commentLines = commentLines
    self.codeLines = codeLines
    self.maxCommentBlockLines = maxCommentBlockLines
    self.containsCommentedOutCode = containsCommentedOutCode
  }

  /// Metrics for a chunk of `language`, or nil when the language has no comment
  /// syntax we model.
  ///
  /// Nil is not "zero comments". Markdown, JSON, and plain text have no comment
  /// concept, and reporting `commentLines: 0` for them would make a
  /// "least-commented code" query rank every prose file first. An unmeasurable
  /// chunk stays unmeasured.
  public static func compute(text: String, language: String?) -> ChunkStructureMetrics? {
    guard let syntax = CommentSyntax.forLanguage(language) else { return nil }
    return compute(text: text, syntax: syntax)
  }

  static func compute(text: String, syntax: CommentSyntax) -> ChunkStructureMetrics {
    var commentLines = 0
    var codeLines = 0
    var maxBlock = 0
    var currentBlock = 0
    var hasCommentedOutCode = false
    /// Consecutive comment lines carrying a weak code signal. A single
    /// prose-shaped line ("Filename = SHA-256 of the node id") is
    /// indistinguishable from an assignment; two in a row is a block.
    var weakRun = 0
    /// Block-comment nesting/state carried across lines.
    var openBlockTerminator: String?

    func note(_ likelihood: CodeLikelihood) {
      switch likelihood {
      case .strong:
        hasCommentedOutCode = true
        weakRun = 0
      case .weak:
        weakRun += 1
        if weakRun >= 2 { hasCommentedOutCode = true }
      case .none:
        weakRun = 0
      }
    }

    for rawLine in text.split(separator: "\n", omittingEmptySubsequences: false) {
      let line = String(rawLine)
      let trimmed = line.trimmingCharacters(in: .whitespaces)

      // Inside a block comment: everything up to the terminator is comment.
      if let terminator = openBlockTerminator {
        commentLines += 1
        currentBlock += 1
        maxBlock = max(maxBlock, currentBlock)
        if let range = line.range(of: terminator) {
          openBlockTerminator = nil
          let tail = String(line[range.upperBound...]).trimmingCharacters(in: .whitespaces)
          // Code after the terminator on the same line makes it a code line too.
          if !tail.isEmpty { codeLines += 1 }
        } else {
          note(codeLikelihood(commentBody(of: trimmed, syntax: syntax)))
        }
        continue
      }

      if trimmed.isEmpty {
        // Blank lines break a comment run without counting as either kind.
        currentBlock = 0
        weakRun = 0
        continue
      }

      let classification = classify(line: line, trimmed: trimmed, syntax: syntax)
      switch classification.kind {
      case .comment:
        commentLines += 1
        currentBlock += 1
        maxBlock = max(maxBlock, currentBlock)
        note(classification.commentBody.isEmpty ? .none : codeLikelihood(classification.commentBody))
      case .code:
        codeLines += 1
        currentBlock = 0
        weakRun = 0
      case .codeWithTrailingComment:
        // A trailing comment does not make the line a comment line, and must not
        // extend a comment block — otherwise every commented-assignment run in a
        // struct reads as one long comment block.
        codeLines += 1
        currentBlock = 0
        weakRun = 0
      }
      openBlockTerminator = classification.opensBlock
    }

    return ChunkStructureMetrics(
      commentLines: commentLines,
      codeLines: codeLines,
      maxCommentBlockLines: maxBlock,
      containsCommentedOutCode: hasCommentedOutCode
    )
  }

  // MARK: - Line classification

  enum LineKind {
    case comment
    case code
    case codeWithTrailingComment
  }

  struct Classification {
    let kind: LineKind
    /// Comment text with its marker stripped, for the commented-out-code test.
    let commentBody: String
    /// Terminator to look for if this line opened an unterminated block comment.
    let opensBlock: String?
  }

  /// Classify one line, scanning left to right so string literals win.
  ///
  /// The scan is what keeps `let url = "http://example.com"` out of the comment
  /// count. A marker search that ignores quoting reports that line as a trailing
  /// comment, and URLs are common enough that the ratio would be visibly wrong
  /// on exactly the web-facing code most likely to be audited.
  static func classify(line: String, trimmed: String, syntax: CommentSyntax) -> Classification {
    // A line-comment marker at position 0 (after indentation) is the common
    // case and needs no scan.
    for marker in syntax.lineMarkers where trimmed.hasPrefix(marker) {
      return Classification(
        kind: .comment,
        commentBody: strippingCommentDecoration(String(trimmed.dropFirst(marker.count))),
        opensBlock: nil
      )
    }
    for (opener, closer) in syntax.blockDelimiters where trimmed.hasPrefix(opener) {
      let afterOpener = String(trimmed.dropFirst(opener.count))
      if let closeRange = afterOpener.range(of: closer) {
        let tail = String(afterOpener[closeRange.upperBound...]).trimmingCharacters(in: .whitespaces)
        return Classification(
          kind: tail.isEmpty ? .comment : .codeWithTrailingComment,
          commentBody: String(afterOpener[..<closeRange.lowerBound]).trimmingCharacters(in: .whitespaces),
          opensBlock: nil
        )
      }
      return Classification(
        kind: .comment,
        commentBody: afterOpener.trimmingCharacters(in: .whitespaces),
        opensBlock: closer
      )
    }

    // Mid-line: scan for an unquoted marker.
    let characters = Array(line)
    var index = 0
    var quote: Character?
    while index < characters.count {
      let char = characters[index]
      if let active = quote {
        if char == "\\" { index += 2; continue }
        if char == active { quote = nil }
        index += 1
        continue
      }
      if char == "\"" || char == "'" || char == "`" {
        quote = char
        index += 1
        continue
      }
      if let marker = syntax.lineMarkers.first(where: { matches($0, in: characters, at: index) }) {
        let body = String(characters[(index + marker.count)...]).trimmingCharacters(in: .whitespaces)
        return Classification(kind: .codeWithTrailingComment, commentBody: body, opensBlock: nil)
      }
      if let (opener, closer) = syntax.blockDelimiters.first(where: { matches($0.0, in: characters, at: index) }) {
        let rest = String(characters[(index + opener.count)...])
        if rest.range(of: closer) != nil {
          // Opened and closed on this line — the line is still code.
          return Classification(kind: .codeWithTrailingComment, commentBody: "", opensBlock: nil)
        }
        return Classification(kind: .codeWithTrailingComment, commentBody: "", opensBlock: closer)
      }
      index += 1
    }

    return Classification(kind: .code, commentBody: "", opensBlock: nil)
  }

  private static func matches(_ marker: String, in characters: [Character], at index: Int) -> Bool {
    let markerChars = Array(marker)
    guard index + markerChars.count <= characters.count else { return false }
    for offset in 0..<markerChars.count where characters[index + offset] != markerChars[offset] {
      return false
    }
    return true
  }

  /// Strip any leading comment marker from an already-trimmed line.
  static func commentBody(of trimmed: String, syntax: CommentSyntax) -> String {
    for marker in syntax.lineMarkers where trimmed.hasPrefix(marker) {
      return strippingCommentDecoration(String(trimmed.dropFirst(marker.count)))
    }
    // Inside a block comment, leading decoration ("*", "///") is not a marker.
    return strippingCommentDecoration(trimmed)
  }

  /// Remove doc-comment decoration left over after the marker.
  ///
  /// `lineMarkers` holds `//`, so a `///` doc comment leaves a body starting
  /// `/ …`. Every pattern below is anchored at `^`, so that stray character
  /// silently defeated all of them — and worse, `/ {` then matched a
  /// "line ends in a brace" rule. Doc styles that need this: `///`, `//!`,
  /// `/**`, `##`, `#!`, and the `*` rail inside a block comment.
  private static func strippingCommentDecoration(_ body: String) -> String {
    var result = Substring(body)
    while let first = result.first, first == "/" || first == "*" || first == "!" || first == "#" {
      result = result.dropFirst()
    }
    return String(result).trimmingCharacters(in: .whitespaces)
  }

  // MARK: - Commented-out code

  /// Statement shapes that a prose sentence does not accidentally produce.
  ///
  /// Prose is the whole difficulty. "if we do X then Y" opens with a keyword,
  /// and a bare keyword test flags most explanatory comments — which would make
  /// the signal useless on well-commented code, the opposite of the intent. So
  /// every pattern here demands punctuation or structure that prose does not
  /// carry: a call's parentheses, an assignment's `=`, a brace, a terminator.
  /// Patterns whose match is enough on its own.
  ///
  /// Each demands structure a wrapped English sentence does not produce: a
  /// declaration header, a semicolon-terminated statement, a call with no space
  /// before its parenthesis.
  private static let strongCodePatterns: [NSRegularExpression] = compile([
    // Declarations: `def foo(`, `func foo(`, `function foo(`, `fn foo(`.
    // The optional modifier run matters — `public func`, `private static func`,
    // and `public struct` are how most real declarations actually appear, and
    // without it recall stalled at 86% with every miss an access modifier.
    #"^(MODS)*(def|func|function|fn|sub)\s+[A-Za-z_$][\w$]*\s*[(<]"#,
    // Type declarations followed by a brace or inheritance.
    #"^(MODS)*(class|struct|enum|interface|trait|impl|module|protocol|actor)\s+[A-Za-z_$][\w$]*\s*[:{<(]"#,
    // `extension Foo {`, `namespace Bar {`. Unambiguous: prose does not open
    // with a type name and a brace.
    #"^(extension|namespace)\s+[A-Za-z_$][\w$.]*\s*[:{<]"#,
    // A call statement. No space before `(`: prose parenthesises constantly —
    // "Swift (SwiftLint)", "Settings (opens macOS Settings window)", "Peel
    // (macOS)" all read as a call once a space is allowed, and each is a real
    // comment from this corpus that the looser pattern flagged.
    #"^[A-Za-z_$][\w$]*(\.[A-Za-z_$][\w$]*)*\([^)]*\)\s*[;,{]?\s*$"#,
    // NOTE: no "ends in a semicolon" rule, in any form. It is the most tempting
    // signal here and the worst one, because engineers use the semicolon as a
    // clause separator in prose: "The actor is the source of truth;",
    // "Labels carry the verdict signal for the auto-merge gate;". A first
    // attempt used `[;{]$` and flagged 20% of files, almost all sentences; a
    // second tried to rescue it with `[)\]\w]\s*;$` and flagged 15%, the same
    // sentences. Semicolon-terminated code is already caught by the call and
    // declaration rules.
    // A typed property declaration: `public let path: String`, `var m: String?`.
    // Strong rather than weak, and deliberately ahead of the prose guard: an
    // optional type ends in `?` or `!`, which the guard reads as sentence
    // punctuation. As a weak pattern it was unreachable for exactly the
    // declarations that are most common in a Swift corpus.
    #"^(MODS)*(let|var|const|val)\s+[A-Za-z_$][\w$]*\s*:\s*\S"#,
    // An assignment whose right-hand side is a call: `let x = store.lookup(k)`.
    // Strong where a bare assignment is weak, because the call's parentheses are
    // what prose lacks — "Filename = SHA-256 of the project's node id" has an
    // equals sign but no call.
    #"^(MODS)*(let|var|const|val|final)?\s*[A-Za-z_$][\w$.\[\]]*\s*=\s*[A-Za-z_$][\w$.]*\([^)]*\)\s*;?\s*$"#,
    // Control flow carrying code punctuation.
    #"^(if|elsif|elif|unless|while|until|for|foreach|switch|match|catch)\s*\(.*\)\s*[{:]?\s*$"#,
    // Imports, tightened to real import shapes. Bare `from`/`use`/`include` are
    // ordinary English words that begin wrapped comment lines constantly.
    #"^import\s+[A-Za-z_$][\w$.]*\s*;?\s*$"#,
    #"^from\s+[\w./]+\s+import\s+\S"#,
    #"^(require|require_relative)\s*\(?['"]"#,
    #"^#include\s*[<"]"#,
    // Annotations/decorators.
    #"^[@#]\[?[A-Za-z_$][\w$.]*(\(.*\))?\]?\s*$"#,
  ])

  /// Patterns that are code-shaped but also occur in prose, so they only count
  /// as part of a run of consecutive comment lines.
  private static let weakCodePatterns: [NSRegularExpression] = compile([
    // Assignment. "Filename = SHA-256 of the project's node id" is prose with
    // an equals sign in it, and there is no way to tell it from `x = y` on one
    // line alone — but two of them in a row is a commented-out block.
    #"^((let|var|const|final|my|our)\s+)?[A-Za-z_$][\w$.\[\]"']*\s*(=|\+=|-=|\|\|=|\?\?=|:=)\s*\S"#,
    // Returns/raises with a value. All four words open English clauses.
    #"^(return|throw|raise|yield|await)\s+\S"#,
    // Bare block terminators, including stacked closers like `});` and `],`.
    #"^(end|fi|done|esac)\s*$"#,
    #"^[)\}\]][)\}\],;]*\s*$"#,
    #"^(if|unless|while|until|for)\s+.+\s+(do|then)\s*$"#,
    // Fragments of a multi-line construct. Individually these are nothing; in a
    // run they are the most common real shape of commented-out code, which is
    // usually a whole block rather than one statement. Recall was 66% without
    // them, and every miss was a fragment: `let logger = Logger(` followed by
    // `subsystem: "com.peel",`, or the elements of a commented-out array.
    //
    // A labelled argument: `subsystem: "com.peel",`.
    #"^[A-Za-z_$][\w$]*\s*:\s*\S.*,\s*$"#,
    // A string or number literal element: `"device.repos.assign",`.
    #"^([""'].*[""']|[\d_.]+)\s*,\s*$"#,
    // A line that opens a bracket it does not close — a call or literal spread
    // over several lines.
    #"^\S.*[({\[]\s*$"#,
    // An enum case: `case idle`, `case queueFull`. Lowercase only, so the
    // sentence "Case sensitivity matters" does not match.
    #"^case\s+[A-Za-z_$][\w$]*\s*[,({:]?\s*$"#,
  ])

  /// Access/scope modifiers that may precede a declaration, as a regex group.
  /// Spliced into the patterns above so the list is written once.
  private static let modifierGroup =
    "(?:public|private|internal|fileprivate|open|final|static|class|export|default|"
    + "abstract|override|mutating|nonisolated|@objc|async|throws)\\s+"

  private static func compile(_ sources: [String]) -> [NSRegularExpression] {
    sources.compactMap {
      try? NSRegularExpression(
        pattern: $0.replacingOccurrences(of: "MODS", with: modifierGroup),
        options: []
      )
    }
  }

  enum CodeLikelihood {
    case none
    /// Code-shaped, but the shape also occurs in prose. Needs corroboration
    /// from an adjacent comment line.
    case weak
    /// Structure prose does not produce. Stands alone.
    case strong
  }

  /// How much a comment body reads as code rather than prose.
  static func codeLikelihood(_ body: String) -> CodeLikelihood {
    let candidate = body.trimmingCharacters(in: .whitespaces)
    guard candidate.count >= 3, candidate.count <= 400 else { return .none }

    // Directive comments are structured metadata, not abandoned code.
    let lowered = candidate.lowercased()
    for directive in ["todo", "fixme", "note", "warning", "mark", "swiftlint",
                      "eslint", "prettier", "rubocop", "noqa", "pragma", "pylint",
                      "type:", "swiftformat", "codestyle", "coverage"]
    where lowered.hasPrefix(directive) {
      return .none
    }

    let range = NSRange(candidate.startIndex..., in: candidate)
    // Strong patterns are checked BEFORE the prose-punctuation guard, because
    // the guard's terminators are also Swift syntax: `public var model: String?`
    // and `let name: String!` end in `?` and `!`. Guarding first silently
    // discarded every optional-typed declaration — a large share of the real
    // commented-out code in a Swift corpus. Nothing strong matches a sentence:
    // each demands a declaration header or a space-free call.
    if strongCodePatterns.contains(where: { $0.firstMatch(in: candidate, options: [], range: range) != nil }) {
      return .strong
    }

    // A sentence ending in prose punctuation is prose, even if it opens with a
    // keyword: "if the peer drops, retry." The weak patterns are exactly the
    // ones a sentence can trip, so the guard sits in front of them.
    if candidate.hasSuffix(".") || candidate.hasSuffix("?") || candidate.hasSuffix("!") {
      // ...unless the "sentence" is a method chain or a call ending in `);`.
      if !candidate.hasSuffix(");") && !candidate.hasSuffix("};") { return .none }
    }

    if weakCodePatterns.contains(where: { $0.firstMatch(in: candidate, options: [], range: range) != nil }) {
      return .weak
    }
    return .none
  }

  /// Convenience for the strong-enough-alone test.
  static func looksLikeCode(_ body: String) -> Bool {
    codeLikelihood(body) == .strong
  }
}

/// Comment syntax for a language, as far as line counting needs it.
///
/// Deliberately not a parser. Counting comment lines does not need to know that
/// a Python `"""` may be a docstring or a string expression; it needs to know
/// which markers open and close comments, and a full lexer per language is a
/// cost with no matching benefit here.
public struct CommentSyntax: Sendable, Equatable {
  public let lineMarkers: [String]
  public let blockDelimiters: [(String, String)]

  public init(lineMarkers: [String], blockDelimiters: [(String, String)]) {
    self.lineMarkers = lineMarkers
    self.blockDelimiters = blockDelimiters
  }

  public static func == (lhs: CommentSyntax, rhs: CommentSyntax) -> Bool {
    lhs.lineMarkers == rhs.lineMarkers
      && lhs.blockDelimiters.map(\.0) == rhs.blockDelimiters.map(\.0)
      && lhs.blockDelimiters.map(\.1) == rhs.blockDelimiters.map(\.1)
  }

  static let cFamily = CommentSyntax(lineMarkers: ["//"], blockDelimiters: [("/*", "*/")])
  static let hash = CommentSyntax(lineMarkers: ["#"], blockDelimiters: [])
  static let ruby = CommentSyntax(lineMarkers: ["#"], blockDelimiters: [("=begin", "=end")])
  static let python = CommentSyntax(lineMarkers: ["#"], blockDelimiters: [("\"\"\"", "\"\"\""), ("'''", "'''")])
  static let sql = CommentSyntax(lineMarkers: ["--"], blockDelimiters: [("/*", "*/")])
  static let css = CommentSyntax(lineMarkers: ["//"], blockDelimiters: [("/*", "*/")])
  static let markup = CommentSyntax(lineMarkers: [], blockDelimiters: [("<!--", "-->")])
  static let handlebars = CommentSyntax(lineMarkers: [], blockDelimiters: [("{{!--", "--}}"), ("{{!", "}}")])
  static let erb = CommentSyntax(lineMarkers: [], blockDelimiters: [("<%#", "%>"), ("<!--", "-->")])
  static let lua = CommentSyntax(lineMarkers: ["--"], blockDelimiters: [("--[[", "]]")])

  /// Syntax for a language name as `RAGFileScanner.languageFor` reports it, or
  /// nil when the language has no comment concept.
  ///
  /// Markdown, JSON, and plain text return nil on purpose: they are prose or
  /// data, and a zero comment count for them is a lie that skews any
  /// ratio-ordered query. JSON5/JSONC do support comments, but the scanner
  /// folds them into the same "JSON" label, so they follow the strict reading.
  public static func forLanguage(_ language: String?) -> CommentSyntax? {
    guard let language else { return nil }
    switch language {
    case "Swift", "JavaScript", "TypeScript", "Glimmer TypeScript", "Glimmer JavaScript",
         "Vue", "Svelte", "Astro", "Rust", "Go", "C", "C++", "Java", "Kotlin",
         "Scala", "Groovy", "GraphQL", "Prisma", "Dart", "C#", "Objective-C",
         "PHP", "Zig":
      return .cFamily
    case "Ruby":
      return .ruby
    case "Python":
      return .python
    case "Shell", "PowerShell", "YAML", "TOML", "Perl", "R", "Makefile", "Dockerfile", "Elixir":
      return .hash
    case "SQL":
      return .sql
    case "CSS":
      return .css
    case "HTML", "XML", "SVG":
      return .markup
    case "Handlebars":
      return .handlebars
    case "ERB":
      return .erb
    case "Lua":
      return .lua
    default:
      return nil
    }
  }
}
