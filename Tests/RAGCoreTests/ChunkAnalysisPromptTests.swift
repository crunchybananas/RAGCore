@testable import RAGCore
import Foundation
import Testing

@Suite("Chunk analysis reply parsing")
struct ChunkAnalysisResponseParserTests {
  @Test("A bare JSON object parses")
  func bareObject() throws {
    let analysis = try ChunkAnalysisResponseParser.parse(
      #"{"summary": "Validates the signup form and flags duplicate emails.", "tags": ["validation", "signup"]}"#)
    #expect(analysis.summary == "Validates the signup form and flags duplicate emails.")
    #expect(analysis.tags == ["validation", "signup"])
  }

  @Test("Fences, a preamble, and trailing text around the object are ignored")
  func wrappedObject() throws {
    let reply = """
      Here is the analysis:
      ```json
      {
        "summary": "Formats invoice totals with the account currency.",
        "tags": ["billing", "formatting"]
      }
      ```
      Let me know if you need anything else. {"note": "extra"}
      """
    let analysis = try ChunkAnalysisResponseParser.parse(reply)
    #expect(analysis.summary == "Formats invoice totals with the account currency.")
    #expect(analysis.tags == ["billing", "formatting"])
  }

  @Test("A second object after the first no longer turns the reply into a raw-text summary")
  func twoObjects() throws {
    // The old parser required the WHOLE reply to be one object; this shape was
    // stored verbatim (first 200 chars) as the summary.
    let reply = #"{"summary": "Builds the report query from saved filters.", "tags": ["reporting"]}"# + "\n"
      + #"{"summary": "duplicate", "tags": []}"#
    let analysis = try ChunkAnalysisResponseParser.parse(reply)
    #expect(analysis.summary == "Builds the report query from saved filters.")
  }

  @Test("An invalid escape inside a quoted regex is repaired, not fatal")
  func invalidEscape() throws {
    let reply = #"{"summary": "Extracts the numeric id with /(\d+)$/ from the route path.", "tags": ["routing"]}"#
    let analysis = try ChunkAnalysisResponseParser.parse(reply)
    #expect(analysis.summary.contains("numeric id"))
    #expect(analysis.tags == ["routing"])
  }

  @Test("A double-encoded reply is unwrapped")
  func doubleEncoded() throws {
    let inner = #"{\"summary\": \"Schedules the nightly export job.\", \"tags\": [\"jobs\"]}"#
    let analysis = try ChunkAnalysisResponseParser.parse(#"{"summary": ""# + inner + #"", "tags": []}"#)
    #expect(analysis.summary == "Schedules the nightly export job.")
    #expect(analysis.tags == ["jobs"])
  }

  @Test("A closed reasoning block is stripped; an unclosed one throws")
  func reasoningBlocks() throws {
    let closed = "<think>the user wants JSON</think>" + #"{"summary": "Parses CSV rows into Person models.", "tags": ["csv"]}"#
    #expect(try ChunkAnalysisResponseParser.parse(closed).summary == "Parses CSV rows into Person models.")
    #expect(throws: ChunkAnalysisResponseParser.Failure.unfinishedReasoning) {
      try ChunkAnalysisResponseParser.parse("<think>still thinking about the")
    }
  }

  @Test("A reply truncated mid-summary throws instead of storing half a sentence")
  func truncatedReply() {
    #expect(throws: ChunkAnalysisResponseParser.Failure.self) {
      try ChunkAnalysisResponseParser.parse(#"{"summary": "A Glimmer modifier that"#)
    }
  }

  @Test("A terminated summary survives a truncated tags array")
  func salvagedSummary() throws {
    let analysis = try ChunkAnalysisResponseParser.parse(
      #"{"summary": "Renders the payment history table with yearly totals.", "tags": ["payments", "tab"#)
    #expect(analysis.summary == "Renders the payment history table with yearly totals.")
  }

  @Test("Plain prose from a model that ignored the JSON instruction is accepted")
  func plainProse() throws {
    let analysis = try ChunkAnalysisResponseParser.parse(
      "Computes a compact diff between two strings, grouping token changes with context.")
    #expect(analysis.summary.hasPrefix("Computes a compact diff"))
    #expect(analysis.tags.isEmpty)
  }

  @Test("Empty and summary-less replies throw")
  func unusable() {
    #expect(throws: ChunkAnalysisResponseParser.Failure.empty) { try ChunkAnalysisResponseParser.parse("  \n ") }
    #expect(throws: ChunkAnalysisResponseParser.Failure.self) {
      try ChunkAnalysisResponseParser.parse(#"{"tags": ["only-tags"]}"#)
    }
  }

  @Test("Tags are normalized to unique kebab-case")
  func tagNormalization() {
    let tags = ChunkAnalysisResponseParser.normalizedTags(
      ["Form Validation", "form_validation", " UI/UX ", "", "c++", "--edge--"])
    #expect(tags == ["form-validation", "ui-ux", "c++", "edge"])
  }

  @Test("Whitespace collapses and over-long summaries clip at a sentence end")
  func summaryShape() throws {
    let long = String(repeating: "Loads the account dashboard widgets. ", count: 40)
    let analysis = try ChunkAnalysisResponseParser.parse(#"{"summary": ""# + long + #"", "tags": []}"#)
    #expect(analysis.summary.count <= ChunkAnalysisResponseParser.maximumSummaryLength)
    #expect(analysis.summary.hasSuffix("."))
    #expect(!analysis.summary.contains("  "))
  }
}

@Suite("Chunk analysis prompt")
struct ChunkAnalysisPromptTests {
  private func context(
    _ path: String?, _ language: String?, _ type: String? = nil, _ name: String? = nil
  ) -> ChunkAnalysisContext {
    ChunkAnalysisContext(filePath: path, constructType: type, constructName: name, language: language)
  }

  @Test("The user message carries the file path and construct")
  func carriesPath() {
    let message = ChunkAnalysisPrompt.userMessage(
      chunk: "final class Cart {}", context: context("Sources/Shop/Cart.swift", "Swift", "classDecl", "Cart"))
    #expect(message.contains("File: Sources/Shop/Cart.swift"))
    #expect(message.contains("Language: Swift"))
    #expect(message.contains("Construct: classDecl Cart"))
    #expect(message.hasSuffix("Code:\nfinal class Cart {}"))
  }

  @Test("An import block says so once and asks for a grouped dependency sentence")
  func importsHint() {
    let message = ChunkAnalysisPrompt.userMessage(
      chunk: "import Foundation", context: context("A.swift", "Swift", "imports", "imports"))
    #expect(message.contains("Construct: imports\n"))
    #expect(message.contains("import block"))
  }

  @Test("Translated UI copy is labeled with its locale and never described as logic")
  func localizationHint() {
    let hints = ChunkAnalysisPrompt.kindHints(context: context("web/translations/fr-ca/general.yaml", "YAML"))
    #expect(hints.count == 1)
    #expect(hints[0].contains("translated UI copy (fr-ca)"))
    let flat = ChunkAnalysisPrompt.kindHints(context: context("addons/common/translations/es-mx.yaml", "YAML"))
    #expect(flat.first?.contains("(es-mx)") == true)
    let config = ChunkAnalysisPrompt.kindHints(context: context("config/deploy.yaml", "YAML"))
    #expect(config.first?.contains("configuration or data") == true)
  }

  @Test("Stylesheets, tests, type declarations and split constructs get their hints")
  func otherHints() {
    #expect(ChunkAnalysisPrompt.kindHints(context: context("app/styles/app.css", "CSS")).first?.contains("stylesheet") == true)
    #expect(ChunkAnalysisPrompt.kindHints(context: context("tests/unit/cart-test.ts", "TypeScript"))
      .contains { $0.contains("This chunk is a test") })
    #expect(ChunkAnalysisPrompt.kindHints(context: context("a.ts", "TypeScript", "protocolDecl", "Signature"))
      .contains { $0.contains("declares a type") })
    #expect(ChunkAnalysisPrompt.kindHints(context: context("a.ts", "TypeScript", "classDecl", "Checkout (part 2/5)"))
      .contains { $0.contains("part 2 of 5") })
    #expect(ChunkAnalysisPrompt.kindHints(context: context("Sources/Cart.swift", "Swift", "function", "total")).isEmpty)
  }

  @Test("Long chunks are cut at the prompt budget and the cut is announced")
  func truncation() {
    let chunk = String(repeating: "x", count: ChunkAnalysisPrompt.maxCodeCharacters + 50)
    let message = ChunkAnalysisPrompt.userMessage(chunk: chunk, context: context("A.swift", "Swift"))
    #expect(message.contains("Code (first \(ChunkAnalysisPrompt.maxCodeCharacters) characters):"))
    #expect(!message.contains(String(repeating: "x", count: ChunkAnalysisPrompt.maxCodeCharacters + 1)))
  }

  @Test("The system prompt forbids guessing and boilerplate openers")
  func systemPromptRules() {
    let prompt = ChunkAnalysisPrompt.systemPrompt
    #expect(prompt.contains("exactly as written in the code"))
    #expect(prompt.contains("Expand one only when the expansion appears in the code itself"))
    #expect(prompt.contains("Never begin with \"This code\""))
  }
}

@Suite("Enrichment text layout")
struct EnrichmentTextTests {
  @Test("The summary leads, so an input cap cuts code rather than the summary")
  func summaryFirst() {
    let code = String(repeating: "let value = compute()\n", count: 1_000)
    let text = RAGStore.enrichedEmbeddingText(
      code: code, summary: "Totals line items for the checkout.", filePath: "Sources/Checkout/Cart.swift",
      constructName: "Cart")
    #expect(text.hasPrefix("File: Sources/Checkout/Cart.swift\nConstruct: Cart\nSummary: Totals line items for the checkout.\n\n"))
    #expect(String(text.prefix(2_000)).contains("Totals line items"))
    #expect(text.hasSuffix(code))
  }

  @Test("An import block's placeholder name is not repeated as a construct")
  func importsName() {
    let text = RAGStore.enrichedEmbeddingText(code: "import A", summary: "Depends on A.", filePath: nil, constructName: "imports")
    #expect(text == "Summary: Depends on A.\n\nimport A")
  }
}

private struct ContextCapturingAnalyzer: ChunkAnalyzer {
  let analyzerName = "capturing-analyzer"
  let shouldFail: Bool
  let seen: SeenContexts

  func analyze(chunk: String, constructType: String?, constructName: String?, language: String?) async throws -> ChunkAnalysis {
    Issue.record("the store must call the context-taking overload")
    return ChunkAnalysis(summary: "unused", tags: [])
  }

  func analyze(chunk: String, context: ChunkAnalysisContext) async throws -> ChunkAnalysis {
    await seen.append(context)
    if shouldFail { throw OllamaError.invalidResponse("unusable") }
    return ChunkAnalysis(summary: "Sums the order lines for \(context.filePath ?? "?").", tags: ["orders"])
  }
}

private actor SeenContexts {
  var values: [ChunkAnalysisContext] = []
  func append(_ context: ChunkAnalysisContext) { values.append(context) }
}

private struct FixedEmbedder: EmbeddingProvider {
  let dimensions = 3
  let modelName = "fixed-embedder"
  func embed(texts: [String]) async throws -> [[Float]] { texts.map { _ in [1, 0, 0] } }
}

@Suite("Store analysis plumbing")
struct StoreAnalysisPlumbingTests {
  private func store(analyzer: ContextCapturingAnalyzer) async throws -> (RAGStore, URL) {
    let url = FileManager.default.temporaryDirectory
      .appendingPathComponent("ragcore-analysis-plumbing-\(UUID().uuidString).sqlite")
    let store = RAGStore(dbURL: url, embeddingProvider: FixedEmbedder(), chunkAnalyzer: analyzer)
    _ = try await store.initialize()
    try await store.upsertRepo(
      id: "r1", name: "shop", rootPath: "/repo/shop", lastIndexedAt: nil, repoIdentifier: "github.com/x/shop",
      embeddingModel: "fixed-embedder", embeddingDimensions: 3)
    try await store.upsertFile(
      id: "f1", repoId: "r1", path: "Sources/Orders/Order.swift", hash: "h1", language: "Swift",
      updatedAt: "2026-01-01", modulePath: nil, featureTags: nil)
    try await store.upsertChunk(
      id: "c1", fileId: "f1", startLine: 1, endLine: 3, text: "func total() -> Int { 0 }", tokenCount: 8,
      constructType: "function", constructName: "total", metadata: nil,
      aiSummary: nil, aiTags: nil, analyzedAt: nil, analyzerModel: nil)
    return (store, url)
  }

  @Test("The analyzer receives the chunk's repository-relative path")
  func pathReachesAnalyzer() async throws {
    let seen = SeenContexts()
    let (store, url) = try await store(analyzer: ContextCapturingAnalyzer(shouldFail: false, seen: seen))
    defer { try? FileManager.default.removeItem(at: url) }
    #expect(try await store.analyzeChunks(repoPath: "/repo/shop") == 1)
    let contexts = await seen.values
    #expect(contexts.first?.filePath == "Sources/Orders/Order.swift")
    #expect(contexts.first?.constructName == "total")
  }

  @Test("A failure is marked by the model column alone and stays retryable")
  func failureLeavesNoSentinelSummary() async throws {
    let seen = SeenContexts()
    let (store, url) = try await store(analyzer: ContextCapturingAnalyzer(shouldFail: true, seen: seen))
    defer { try? FileManager.default.removeItem(at: url) }
    #expect(try await store.analyzeChunks(repoPath: "/repo/shop") == 0)
    let summary = try await store.queryString("SELECT COALESCE(ai_summary, '<null>') FROM chunks WHERE id = 'c1'")
    let model = try await store.queryString("SELECT analyzer_model FROM chunks WHERE id = 'c1'")
    #expect(summary == "<null>")
    #expect(model == "chunk-analyzer-failed")
    #expect(try await store.getUnanalyzedChunkCount(repoPath: "/repo/shop") == 1)
    #expect(try await store.getAnalyzedChunkCount(repoPath: "/repo/shop") == 0)
  }
}

@Suite("Failure sentinel migration")
struct FailureSentinelMigrationTests {
  @Test("v24 turns stored failure sentinels into NULL summaries and search never serves one")
  func migratesSentinels() async throws {
    let url = FileManager.default.temporaryDirectory
      .appendingPathComponent("ragcore-sentinel-\(UUID().uuidString).sqlite")
    defer { try? FileManager.default.removeItem(at: url) }
    let first = RAGStore(dbURL: url, embeddingProvider: FixedEmbedder())
    _ = try await first.initialize()
    try await first.upsertRepo(
      id: "r1", name: "shop", rootPath: "/repo/shop", lastIndexedAt: nil, repoIdentifier: nil,
      embeddingModel: "fixed-embedder", embeddingDimensions: 3)
    try await first.upsertFile(
      id: "f1", repoId: "r1", path: "A.swift", hash: "h1", language: "Swift",
      updatedAt: "2026-01-01", modulePath: nil, featureTags: nil)
    try await first.upsertChunk(
      id: "c1", fileId: "f1", startLine: 1, endLine: 1, text: "let failed = true", tokenCount: 4,
      constructType: nil, constructName: nil, metadata: nil,
      aiSummary: "[analysis-failed]", aiTags: nil, analyzedAt: "2026-01-01", analyzerModel: "chunk-analyzer-failed")
    // Pretend this database predates v24.
    try await first.exec("INSERT OR REPLACE INTO rag_meta (key, value) VALUES ('schema_version', '23')")

    let reopened = RAGStore(dbURL: url, embeddingProvider: FixedEmbedder())
    _ = try await reopened.initialize()
    let summary = try await reopened.queryString("SELECT COALESCE(ai_summary, '<null>') FROM chunks WHERE id = 'c1'")
    #expect(summary == "<null>")
    #expect(try await reopened.queryString("SELECT value FROM rag_meta WHERE key = 'schema_version'") == "24")
    #expect(RAGStore.usableSummary("[analysis-failed]") == nil)
    #expect(RAGStore.usableSummary("Real summary.") == "Real summary.")
  }
}
