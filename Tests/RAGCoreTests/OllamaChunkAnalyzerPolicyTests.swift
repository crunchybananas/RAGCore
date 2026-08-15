//
//  OllamaChunkAnalyzerPolicyTests.swift
//  RAGCoreTests
//
//  Pins the analyzer's request policy (#22, #23): the /no_think guard must
//  match model IDENTITY (namespaced pulls prefix the source), and the
//  timeout error must be its own attributable case.
//

import XCTest
@testable import RAGCore

final class OllamaChunkAnalyzerPolicyTests: XCTestCase {

  // MARK: - /no_think identity matching (#23)

  func testBareQwen3TagsWantThePrefix() {
    XCTAssertTrue(OllamaChunkAnalyzer.wantsNoThinkPrefix(model: "qwen3:8b"))
    XCTAssertTrue(OllamaChunkAnalyzer.wantsNoThinkPrefix(model: "qwen3-coder:latest"))
    XCTAssertTrue(OllamaChunkAnalyzer.wantsNoThinkPrefix(model: "Qwen3.5:latest"))
  }

  func testNamespacedQwen3TagsWantThePrefixToo() {
    // The exact tags the old whole-string hasPrefix check missed (#23).
    XCTAssertTrue(OllamaChunkAnalyzer.wantsNoThinkPrefix(
      model: "hf.co/unsloth/Qwen3-30B-A3B-GGUF:Q4_K_M"))
    XCTAssertTrue(OllamaChunkAnalyzer.wantsNoThinkPrefix(
      model: "moophlo/Qwen3-Coder-30B-A3B-Instruct-GGUF:Q3_K_M"))
  }

  func testEmbeddingModelsNeverWantThePrefix() {
    XCTAssertFalse(OllamaChunkAnalyzer.wantsNoThinkPrefix(model: "qwen3-embedding:0.6b"))
    XCTAssertFalse(OllamaChunkAnalyzer.wantsNoThinkPrefix(
      model: "hf.co/some/Qwen3-Embedding-8B-GGUF"))
  }

  func testNonQwenModelsNeverWantThePrefix() {
    XCTAssertFalse(OllamaChunkAnalyzer.wantsNoThinkPrefix(model: "gemma4:26b"))
    XCTAssertFalse(OllamaChunkAnalyzer.wantsNoThinkPrefix(model: "phi4:latest"))
    // A namespace that happens to contain qwen3 must not fool the identity
    // check when the model itself is something else.
    XCTAssertFalse(OllamaChunkAnalyzer.wantsNoThinkPrefix(model: "qwen3-fan/gemma4:26b"))
  }

  // MARK: - Attributable timeout (#22)

  func testTimeoutErrorNamesTheModelAndDeadline() {
    let error = OllamaError.requestTimedOut(model: "hf.co/ggml-org/Qwen3.8-27B-GGUF:Q4_K_M", seconds: 600)
    let description = error.errorDescription ?? ""
    XCTAssertTrue(description.contains("Qwen3.8-27B"), "the corpus hole must name its model")
    XCTAssertTrue(description.contains("600"), "and the deadline that produced it")
    XCTAssertTrue(description.lowercased().contains("cold model load"),
                  "and point at the usual cause")
  }
}
