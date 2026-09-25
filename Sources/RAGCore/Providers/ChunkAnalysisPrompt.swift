//
//  ChunkAnalysisPrompt.swift
//  RAGCore
//
//  The prompt and response contract for LLM chunk analysis, kept apart from
//  any one provider so the wording, the kind hints and the parser are pinned
//  by tests instead of living inline in a request builder.
//

import Foundation

/// Builds the analyzer prompt for one chunk.
///
/// Summaries exist to be searched: they are indexed for keyword search and
/// embedded next to the code for vector search. So the prompt asks for the
/// identifiers a searcher would type, anchors the summary to the file it came
/// from, and forbids the two habits that poison an index: boilerplate openers
/// that make every summary look alike, and guesses. A model that cannot see
/// what an acronym stands for will confidently invent an expansion, and that
/// invented phrase then matches searches it has nothing to do with.
public enum ChunkAnalysisPrompt {
  /// Characters of chunk text sent to the model. Large enough for a typical
  /// class chunk to arrive whole, small enough to stay well inside a 4K-token
  /// context with the prompt and the reply.
  public static let maxCodeCharacters = 6_000

  public static let systemPrompt = """
    You write the summaries a code search index stores for each chunk of source code. \
    Engineers and coding agents find code by searching these summaries, so they must be \
    specific, literal, and true to the code shown.

    Reply with a JSON object: {"summary": "...", "tags": ["...", "..."]}

    summary: 1-3 sentences, at most 60 words.
    - Say what the code does and what role it plays, using the file path to name the feature, \
    screen, or module it belongs to.
    - Name the identifiers that matter (types, functions, components, routes, services, fields, \
    keys) exactly as written in the code.
    - Begin with the substance. Never begin with "This code", "This file", "This function", \
    "This component", or "The code".
    - Describe only what the code shows. Do not guess at intent or at code that is not shown. \
    Never write "likely", "probably", "appears to", "seems to", "may be", "might", or \
    "commonly used".
    - Keep abbreviations and acronyms exactly as written. Expand one only when the expansion \
    appears in the code itself.

    tags: 3 to 6 lowercase kebab-case tags for the domain and the technical concerns, for \
    example "form-validation", "feature-flag", "date-formatting".
    """

  /// JSON schema for Ollama's structured-output `format` field. Constrained
  /// decoding makes an unparseable reply impossible rather than merely rare.
  public static var responseSchema: [String: Any] {
    [
      "type": "object",
      "properties": [
        "summary": ["type": "string"],
        "tags": ["type": "array", "items": ["type": "string"]],
      ],
      "required": ["summary", "tags"],
    ]
  }

  /// The user message for one chunk.
  public static func userMessage(chunk: String, context: ChunkAnalysisContext) -> String {
    var lines: [String] = []
    if let path = context.filePath, !path.isEmpty { lines.append("File: \(path)") }
    if let language = context.language, !language.isEmpty { lines.append("Language: \(language)") }
    let construct = [context.constructType, context.constructName]
      .compactMap { $0?.isEmpty == false ? $0 : nil }
    // An import block is named "imports" by the chunker; saying it twice is noise.
    let uniqueConstruct = construct.count == 2 && construct[0] == construct[1] ? [construct[0]] : construct
    if !uniqueConstruct.isEmpty { lines.append("Construct: \(uniqueConstruct.joined(separator: " "))") }
    let hints = kindHints(context: context)
    if !hints.isEmpty { lines.append("Note: \(hints.joined(separator: " "))") }
    let truncated = chunk.count > maxCodeCharacters
    let header = truncated ? "Code (first \(maxCodeCharacters) characters):" : "Code:"
    lines.append("\(header)\n\(chunk.prefix(maxCodeCharacters))")
    return lines.joined(separator: "\n")
  }

  // MARK: - Kind hints

  static let typeDeclarationKinds: Set<String> = [
    "protocolDecl", "interface", "typeAlias", "type", "enumDecl", "structDecl",
  ]
  static let dataLanguages: Set<String> = ["YAML", "JSON", "TOML", "XML", "Properties", "INI"]
  static let styleLanguages: Set<String> = ["CSS", "SCSS", "Sass", "Less"]

  /// Short, kind-specific instructions. Without them a model treats an import
  /// block, a type declaration and a file of translated strings exactly like
  /// logic, and describes each as if it implemented the feature it mentions.
  static func kindHints(context: ChunkAnalysisContext) -> [String] {
    var hints: [String] = []
    let path = context.filePath ?? ""
    let language = context.language ?? ""
    let constructType = context.constructType ?? ""

    if constructType == "imports" {
      hints.append(
        "This chunk is the file's import block. In one sentence, say what the file depends on, "
          + "grouping the imports by purpose and naming the most important modules. "
          + "Do not list every import.")
    } else if typeDeclarationKinds.contains(constructType) {
      hints.append(
        "This chunk declares a type. Say what it describes, which component, route, or service "
          + "in this file uses it, and name its key fields.")
    }

    if dataLanguages.contains(language) {
      if isLocalizationPath(path) {
        let locale = localeCode(in: path).map { " (\($0))" } ?? ""
        hints.append(
          "This chunk is translated UI copy\(locale): strings, not logic. Say which screens or "
            + "features the strings are for and name the top-level keys. Do not describe the copy "
            + "as implementing those features.")
      } else {
        hints.append(
          "This chunk is configuration or data, not executable code. Say what it configures or "
            + "contains and name the top-level keys.")
      }
    } else if styleLanguages.contains(language) {
      hints.append(
        "This chunk is a stylesheet. Say which components, screens, or elements it styles, "
          + "naming the selectors or classes that matter.")
    }

    if isTestPath(path) {
      hints.append("This chunk is a test. Say which behavior it verifies and which unit it exercises.")
    }

    if let (part, total) = partNumber(in: context.constructName ?? "") {
      hints.append(
        "This is part \(part) of \(total) of a larger construct. Summarize what this part "
          + "contributes to it.")
    }
    return hints
  }

  static func isLocalizationPath(_ path: String) -> Bool {
    let components = path.lowercased().split(separator: "/").map(String.init)
    return components.dropLast().contains { ["translations", "locales", "locale", "i18n", "lang", "intl", "l10n"].contains($0) }
      || components.last.map { $0.hasPrefix("messages.") || $0.hasPrefix("strings.") } == true
  }

  /// A BCP-47-ish locale code in the path: a directory (`fr-ca/`) or a file
  /// stem (`es-mx.yaml`), whichever comes last.
  static func localeCode(in path: String) -> String? {
    let pattern = #"(?:^|/)([a-z]{2}(?:[-_][A-Za-z]{2,4})?)(?:/|\.(?:ya?ml|json|po|properties|strings|xliff|ftl)$)"#
    guard let regex = try? NSRegularExpression(pattern: pattern, options: [.caseInsensitive]) else { return nil }
    let range = NSRange(path.startIndex..., in: path)
    guard let match = regex.matches(in: path, range: range).last,
          let codeRange = Range(match.range(at: 1), in: path) else { return nil }
    return String(path[codeRange])
  }

  static func isTestPath(_ path: String) -> Bool {
    let lowered = path.lowercased()
    if lowered.split(separator: "/").dropLast().contains(where: { ["test", "tests", "spec", "specs", "__tests__"].contains($0) }) {
      return true
    }
    let file = lowered.split(separator: "/").last.map(String.init) ?? lowered
    return file.contains("-test.") || file.contains(".test.") || file.contains("_test.")
      || file.contains(".spec.") || file.contains("_spec.") || file.hasSuffix("tests.swift")
  }

  static func partNumber(in constructName: String) -> (Int, Int)? {
    guard let open = constructName.range(of: "(part "),
          let close = constructName.range(of: ")", range: open.upperBound..<constructName.endIndex) else { return nil }
    let pieces = constructName[open.upperBound..<close.lowerBound].split(separator: "/")
    guard pieces.count == 2, let part = Int(pieces[0]), let total = Int(pieces[1]) else { return nil }
    return (part, total)
  }
}

// MARK: - Response parsing

/// Turns an analyzer reply into a `ChunkAnalysis`, or throws.
///
/// The old parser accepted anything: when a reply was not a bare JSON object it
/// stored the first 200 characters of the raw text as the summary. That put
/// literal `{"summary": ...` fragments, half-sentences cut by the token budget,
/// and model preambles into the index as if they were summaries. Because the
/// chunk then counted as analyzed, nothing ever retried it. A reply that
/// yields no usable summary now throws, which the store records as a failed,
/// retryable analysis.
public enum ChunkAnalysisResponseParser {
  public enum Failure: Error, Equatable, LocalizedError {
    case empty
    case unfinishedReasoning
    case noUsableSummary(String)

    public var errorDescription: String? {
      switch self {
      case .empty: "analyzer returned an empty reply"
      case .unfinishedReasoning: "analyzer reply ended inside its reasoning block"
      case .noUsableSummary(let preview): "analyzer reply carried no usable summary: \(preview)"
      }
    }
  }

  static let minimumSummaryLength = 8
  static let maximumSummaryLength = 700
  static let maximumTags = 8

  public static func parse(_ raw: String) throws -> ChunkAnalysis {
    var text = raw.replacingOccurrences(
      of: #"<think>[\s\S]*?</think>"#, with: "", options: .regularExpression)
    if text.contains("<think>") { throw Failure.unfinishedReasoning }
    text = text.trimmingCharacters(in: .whitespacesAndNewlines)
    guard !text.isEmpty else { throw Failure.empty }

    // 1. The first complete JSON object anywhere in the reply: this ignores
    //    code fences, a preamble, and anything the model added afterwards.
    var searchStart = text.startIndex
    while let object = firstJSONObject(in: text, from: &searchStart) {
      if let analysis = analysis(fromJSON: object) { return analysis }
    }

    // 2. A complete "summary" string inside an object that does not parse as
    //    a whole (an invalid escape elsewhere, a truncated tags array).
    if let summary = salvagedSummary(in: text), let analysis = validated(summary: summary, tags: salvagedTags(in: text)) {
      return analysis
    }

    // 3. A plain-prose answer from a model that ignored the JSON instruction.
    let unfenced = stripFences(text)
    if !unfenced.hasPrefix("{"), !unfenced.hasPrefix("["), !unfenced.contains("\"summary\""),
       let analysis = validated(summary: unfenced, tags: []) {
      return analysis
    }

    throw Failure.noUsableSummary(String(text.prefix(120)))
  }

  // MARK: Helpers

  static func analysis(fromJSON object: [String: Any]) -> ChunkAnalysis? {
    guard let summary = object["summary"] as? String else { return nil }
    let tags: [String]
    if let array = object["tags"] as? [Any] {
      tags = array.compactMap { $0 as? String }
    } else if let joined = object["tags"] as? String {
      tags = joined.split(separator: ",").map(String.init)
    } else {
      tags = []
    }
    // A summary that is itself an encoded reply (double-encoded JSON).
    if summary.trimmingCharacters(in: .whitespaces).hasPrefix("{") {
      var start = summary.startIndex
      if let nested = firstJSONObject(in: summary, from: &start), nested["summary"] != nil {
        return analysis(fromJSON: nested)
      }
      return nil
    }
    return validated(summary: summary, tags: tags)
  }

  static func validated(summary raw: String, tags rawTags: [String]) -> ChunkAnalysis? {
    let collapsed = raw.split(whereSeparator: { $0.isWhitespace }).joined(separator: " ")
    guard collapsed.count >= minimumSummaryLength, !collapsed.contains("\"summary\"") else { return nil }
    return ChunkAnalysis(summary: clipped(collapsed), tags: normalizedTags(rawTags))
  }

  /// Clip an over-long summary at the last sentence end that fits.
  static func clipped(_ summary: String) -> String {
    guard summary.count > maximumSummaryLength else { return summary }
    let head = String(summary.prefix(maximumSummaryLength))
    if let end = head.range(of: ". ", options: .backwards), head.distance(from: head.startIndex, to: end.lowerBound) > 40 {
      return String(head[..<end.lowerBound]) + "."
    }
    return head
  }

  static func normalizedTags(_ tags: [String]) -> [String] {
    var seen = Set<String>()
    var result: [String] = []
    for tag in tags {
      var t = tag.lowercased().trimmingCharacters(in: .whitespacesAndNewlines)
      t = t.replacingOccurrences(of: #"[\s_/]+"#, with: "-", options: .regularExpression)
      t = t.replacingOccurrences(of: #"[^a-z0-9.+#-]"#, with: "", options: .regularExpression)
      t = t.replacingOccurrences(of: #"-{2,}"#, with: "-", options: .regularExpression)
      t = t.trimmingCharacters(in: CharacterSet(charactersIn: "-."))
      guard !t.isEmpty, t.count <= 40, seen.insert(t).inserted else { continue }
      result.append(t)
      if result.count == maximumTags { break }
    }
    return result
  }

  static func stripFences(_ text: String) -> String {
    text.replacingOccurrences(of: #"```[A-Za-z0-9_-]*"#, with: "", options: .regularExpression)
      .trimmingCharacters(in: .whitespacesAndNewlines)
  }

  /// The next balanced `{...}` at or after `start` that parses as a JSON
  /// object. Advances `start` past every candidate it examines.
  static func firstJSONObject(in text: String, from start: inout String.Index) -> [String: Any]? {
    while let open = text[start...].firstIndex(of: "{") {
      var depth = 0
      var inString = false
      var escaped = false
      var index = open
      var close: String.Index?
      while index < text.endIndex {
        let character = text[index]
        if inString {
          if escaped { escaped = false }
          else if character == "\\" { escaped = true }
          else if character == "\"" { inString = false }
        } else if character == "\"" {
          inString = true
        } else if character == "{" {
          depth += 1
        } else if character == "}" {
          depth -= 1
          if depth == 0 { close = index; break }
        }
        index = text.index(after: index)
      }
      guard let close else {
        start = text.endIndex
        return nil
      }
      start = text.index(after: open)
      let candidate = String(text[open...close])
      if let object = jsonObject(candidate) ?? jsonObject(repairedEscapes(candidate)) {
        start = text.index(after: close)
        return object
      }
    }
    start = text.endIndex
    return nil
  }

  /// A terminated `"summary": "..."` string, decoded. A truncated one (the
  /// token budget ran out mid-sentence) has no closing quote and is refused:
  /// half a sentence is worse than a retry.
  static func salvagedSummary(in text: String) -> String? {
    guard let regex = try? NSRegularExpression(pattern: #""summary"\s*:\s*"((?:[^"\\]|\\.)*)""#),
          let match = regex.firstMatch(in: text, range: NSRange(text.startIndex..., in: text)),
          let range = Range(match.range(at: 1), in: text) else { return nil }
    let literal = "\"\(text[range])\""
    return jsonString(literal) ?? jsonString(repairedEscapes(literal))
  }

  static func jsonObject(_ text: String) -> [String: Any]? {
    guard let data = text.data(using: .utf8) else { return nil }
    return (try? JSONSerialization.jsonObject(with: data)) as? [String: Any]
  }

  static func jsonString(_ literal: String) -> String? {
    guard let data = literal.data(using: .utf8) else { return nil }
    return (try? JSONSerialization.jsonObject(with: data, options: [.fragmentsAllowed])) as? String
  }

  /// Double every backslash that does not start a valid JSON escape. Models
  /// quote code inside summaries (a regex like `\d+`, a Windows path), and a
  /// single invalid escape otherwise makes the whole reply unparseable.
  static func repairedEscapes(_ text: String) -> String {
    text.replacingOccurrences(of: #"\\(?!["\\/bfnrtu])"#, with: #"\\\\"#, options: .regularExpression)
  }

  static func salvagedTags(in text: String) -> [String] {
    guard let regex = try? NSRegularExpression(pattern: #""tags"\s*:\s*\[([^\]]*)\]"#),
          let match = regex.firstMatch(in: text, range: NSRange(text.startIndex..., in: text)),
          let range = Range(match.range(at: 1), in: text) else { return [] }
    let literal = "[\(text[range])]"
    guard let data = literal.data(using: .utf8),
          let array = try? JSONSerialization.jsonObject(with: data) as? [Any] else { return [] }
    return array.compactMap { $0 as? String }
  }
}
