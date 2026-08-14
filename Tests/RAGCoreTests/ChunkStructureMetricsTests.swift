//
//  ChunkStructureMetricsTests.swift
//  RAGCoreTests
//

import XCTest
@testable import RAGCore

final class ChunkStructureMetricsTests: XCTestCase {

  private func metrics(_ text: String, _ language: String) -> ChunkStructureMetrics {
    guard let m = ChunkStructureMetrics.compute(text: text, language: language) else {
      XCTFail("expected metrics for \(language)")
      return ChunkStructureMetrics(commentLines: 0, codeLines: 0, maxCommentBlockLines: 0,
                                   containsCommentedOutCode: false)
    }
    return m
  }

  // MARK: - Counting

  func testCountsLineCommentsAndCode() {
    let m = metrics("""
      // one
      // two
      let x = 1
      """, "Swift")

    XCTAssertEqual(m.commentLines, 2)
    XCTAssertEqual(m.codeLines, 1)
    XCTAssertEqual(m.maxCommentBlockLines, 2)
  }

  func testBlankLinesCountAsNeitherAndBreakABlock() {
    let m = metrics("""
      // one

      // two
      let x = 1
      """, "Swift")

    XCTAssertEqual(m.commentLines, 2)
    XCTAssertEqual(m.codeLines, 1)
    XCTAssertEqual(m.maxCommentBlockLines, 1, "a blank line ends the run")
  }

  func testBlockCommentSpansLines() {
    let m = metrics("""
      /*
       * a
       * b
       */
      run()
      """, "Swift")

    XCTAssertEqual(m.commentLines, 4)
    XCTAssertEqual(m.codeLines, 1)
    XCTAssertEqual(m.maxCommentBlockLines, 4)
  }

  func testTrailingCommentIsACodeLine() {
    let m = metrics("""
      let x = 1  // set x
      let y = 2  // set y
      """, "Swift")

    XCTAssertEqual(m.codeLines, 2)
    XCTAssertEqual(m.commentLines, 0)
    XCTAssertEqual(m.maxCommentBlockLines, 0,
                   "trailing comments must not accumulate into a comment block")
  }

  func testRatioIsCommentsOverNonBlankLines() {
    let m = metrics("""
      // a
      // b
      // c
      let x = 1
      """, "Swift")

    XCTAssertEqual(m.commentLineRatio ?? 0, 0.75, accuracy: 0.001)
  }

  func testRatioIsNilWithNoNonBlankLines() {
    let m = metrics("\n\n\n", "Swift")

    XCTAssertNil(m.commentLineRatio, "no lines to take a ratio of is not a ratio of zero")
  }

  // MARK: - The string-literal trap

  /// The case that makes a naive scanner visibly wrong on web-facing code.
  func testURLInAStringIsNotAComment() {
    let m = metrics("""
      let url = "http://example.com/path"
      let other = "https://api.example.com"
      """, "Swift")

    XCTAssertEqual(m.commentLines, 0)
    XCTAssertEqual(m.codeLines, 2)
  }

  func testHashInsideARubyStringIsNotAComment() {
    let m = metrics("""
      name = "#not-a-comment"
      other = 'also # not one'
      """, "Ruby")

    XCTAssertEqual(m.commentLines, 0)
    XCTAssertEqual(m.codeLines, 2)
  }

  func testEscapedQuoteDoesNotLeakStringState() {
    let m = metrics("""
      let a = "she said \\"hi\\"" // real comment
      let b = 2
      """, "Swift")

    XCTAssertEqual(m.codeLines, 2)
    XCTAssertEqual(m.commentLines, 0)
  }

  // MARK: - Commented-out code

  func testDetectsCommentedOutRuby() {
    let m = metrics("""
      # def old_method(arg)
      #   value = compute(arg)
      # end
      def new_method; end
      """, "Ruby")

    XCTAssertTrue(m.containsCommentedOutCode)
  }

  func testDetectsCommentedOutSwiftAssignment() {
    let m = metrics("""
      // let cached = store.lookup(key)
      let fresh = store.fetch(key)
      """, "Swift")

    XCTAssertTrue(m.containsCommentedOutCode)
  }

  func testDetectsCommentedOutCallInsideBlockComment() {
    let m = metrics("""
      /*
      configure(server, port: 8765);
      */
      run()
      """, "Swift")

    XCTAssertTrue(m.containsCommentedOutCode)
  }

  /// The failure that would make this signal useless: flagging ordinary prose
  /// and so marking every well-commented file as a cleanup candidate.
  func testProseIsNotCommentedOutCode() {
    let m = metrics("""
      // If the peer drops mid-request, retry once before giving up.
      // We return early here because the caller already holds the lock.
      // For each worker we track the last heartbeat.
      // Note that this class is not thread safe.
      let x = 1
      """, "Swift")

    XCTAssertFalse(m.containsCommentedOutCode,
                   "prose opening with if/for/return must not read as code")
  }

  func testDirectiveCommentsAreNotCommentedOutCode() {
    let m = metrics("""
      // TODO: rename this once the migration lands
      // MARK: - Helpers
      // swiftlint:disable force_cast
      let x = 1
      """, "Swift")

    XCTAssertFalse(m.containsCommentedOutCode)
  }

  func testEmptyAndTinyCommentsAreNotCode() {
    XCTAssertFalse(ChunkStructureMetrics.looksLikeCode(""))
    XCTAssertFalse(ChunkStructureMetrics.looksLikeCode("x"))
    XCTAssertFalse(ChunkStructureMetrics.looksLikeCode("---"))
  }

  /// A prose sentence that happens to contain an equals sign is still prose.
  func testProseWithAnEqualsSignIsNotCode() {
    XCTAssertFalse(ChunkStructureMetrics.looksLikeCode(
      "the ratio = comments over code, roughly."))
  }

  /// Bare terminators are code-shaped but weak: one `// end` proves nothing,
  /// two in a row is a commented-out block.
  func testBareBlockTerminatorsAreWeakNotStrong() {
    XCTAssertEqual(ChunkStructureMetrics.codeLikelihood("end"), .weak)
    XCTAssertEqual(ChunkStructureMetrics.codeLikelihood("});"), .weak)
  }

  /// A one- or two-character body is too weak to act on. `// }` appears in
  /// ASCII diagrams and box-drawing comments, and one of those in a file would
  /// otherwise flag the whole chunk as containing abandoned code.
  func testSingleCharacterBodiesAreTooWeakToCount() {
    XCTAssertEqual(ChunkStructureMetrics.codeLikelihood("}"), .none)
    XCTAssertEqual(ChunkStructureMetrics.codeLikelihood("|"), .none)
  }

  /// Two weak lines in a row is the run rule doing its job.
  func testTwoConsecutiveWeakLinesDetectABlock() {
    let m = metrics("""
      // model = candidate
      // priority = 3
      run()
      """, "Swift")

    XCTAssertTrue(m.containsCommentedOutCode)
  }

  /// One weak line on its own must not be enough — this is the prose-assignment
  /// case ("Filename = SHA-256 of the node id") that has no tell in isolation.
  func testOneWeakLineAloneIsNotEnough() {
    let m = metrics("""
      // Filename = SHA-256 of the project's node id
      run()
      """, "Swift")

    XCTAssertFalse(m.containsCommentedOutCode)
  }

  /// An optional-typed declaration ends in `?`, which is also prose sentence
  /// punctuation. Guarding on the terminator first discarded every one of them.
  func testOptionalTypedDeclarationIsNotMistakenForAQuestion() {
    XCTAssertEqual(ChunkStructureMetrics.codeLikelihood("public var model: String?"), .strong)
    XCTAssertEqual(ChunkStructureMetrics.codeLikelihood("let name: String!"), .strong)
    XCTAssertEqual(ChunkStructureMetrics.codeLikelihood("is the peer still reachable?"), .none)
  }

  /// The semicolon is a clause separator in engineering prose. Treating it as a
  /// statement terminator flagged 20% of a real corpus, nearly all sentences.
  func testProseEndingInASemicolonIsNotCode() {
    XCTAssertEqual(ChunkStructureMetrics.codeLikelihood("The actor is the source of truth;"), .none)
    XCTAssertEqual(ChunkStructureMetrics.codeLikelihood("Reads are non-blocking;"), .none)
  }

  /// Prose parenthesises constantly; a real call has no space before its paren.
  func testParentheticalProseIsNotACall() {
    XCTAssertEqual(ChunkStructureMetrics.codeLikelihood("Swift (SwiftLint)"), .none)
    XCTAssertEqual(ChunkStructureMetrics.codeLikelihood("Settings (opens macOS Settings window)"), .none)
    XCTAssertEqual(ChunkStructureMetrics.codeLikelihood("StatusChip(\"Idle\", .idle)"), .strong)
  }

  /// `///` leaves a `/` on the body after the `//` marker is removed. Every
  /// pattern is anchored at `^`, so the stray character defeated all of them.
  func testDocCommentSlashIsStrippedFromTheBody() {
    let m = metrics("""
      /// configure(server, port: 8765)
      run()
      """, "Swift")

    XCTAssertTrue(m.containsCommentedOutCode)
  }

  // MARK: - Language coverage

  func testPythonUsesHashAndTripleQuotes() {
    let m = metrics("""
      # comment
      def f():
          pass
      """, "Python")

    XCTAssertEqual(m.commentLines, 1)
    XCTAssertEqual(m.codeLines, 2)
  }

  func testSQLUsesDoubleDash() {
    let m = metrics("""
      -- pick the rows
      SELECT 1;
      """, "SQL")

    XCTAssertEqual(m.commentLines, 1)
    XCTAssertEqual(m.codeLines, 1)
  }

  func testHTMLUsesAngleComments() {
    let m = metrics("""
      <!-- hidden -->
      <div>x</div>
      """, "HTML")

    XCTAssertEqual(m.commentLines, 1)
    XCTAssertEqual(m.codeLines, 1)
  }

  func testRubyBlockComment() {
    let m = metrics("""
      =begin
      old_call(1)
      =end
      run
      """, "Ruby")

    XCTAssertEqual(m.commentLines, 3)
    XCTAssertEqual(m.codeLines, 1)
    XCTAssertTrue(m.containsCommentedOutCode)
  }

  /// Prose and data languages have no comment concept. Reporting zero comments
  /// would make every Markdown file the least-commented "code" in the repo.
  func testProseLanguagesAreUnmeasured() {
    XCTAssertNil(ChunkStructureMetrics.compute(text: "# Heading\ntext", language: "Markdown"))
    XCTAssertNil(ChunkStructureMetrics.compute(text: "{\"a\": 1}", language: "JSON"))
    XCTAssertNil(ChunkStructureMetrics.compute(text: "hello", language: "Text"))
    XCTAssertNil(ChunkStructureMetrics.compute(text: "x", language: nil))
    XCTAssertNil(ChunkStructureMetrics.compute(text: "x", language: "Brainfuck"))
  }

  // MARK: - Realistic shapes

  /// A heavily-commented header over a little code — the ratio query's target.
  func testDocCommentHeaderProducesAHighRatio() {
    let m = metrics("""
      /// Resolves a repo identifier to a local checkout.
      ///
      /// Identifiers are normalized remote URLs. A relative path cannot refer to
      /// the client's working directory, so it searches all repos instead.
      ///
      /// - Parameter id: the identifier to resolve.
      func resolve(_ id: String) -> String? {
        registry.lookup(id)
      }
      """, "Swift")

    XCTAssertEqual(m.commentLines, 6, "four prose lines plus the two bare /// separators")
    XCTAssertEqual(m.codeLines, 3)
    XCTAssertGreaterThan(m.commentLineRatio ?? 0, 0.6)
    XCTAssertFalse(m.containsCommentedOutCode, "doc comments are prose")
  }

  func testUncommentedCodeHasZeroRatio() {
    let m = metrics("""
      func add(_ a: Int, _ b: Int) -> Int {
        a + b
      }
      """, "Swift")

    XCTAssertEqual(m.commentLines, 0)
    XCTAssertEqual(m.commentLineRatio ?? -1, 0.0, accuracy: 0.0001)
    XCTAssertFalse(m.containsCommentedOutCode)
  }
}
