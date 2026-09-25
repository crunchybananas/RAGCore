@testable import RAGCore
import Foundation
import Testing

@Suite("Markdown section chunking")
struct MarkdownSectionChunkerTests {
  private let document = """
    Intro paragraph before any heading.
    It explains the document.

    # Design System

    ## Data layer
    ### Requests
    Requests live in the data addon.
    Each request builder returns a typed response.

    ```bash
    # not a heading, just a shell comment
    pnpm test
    ```

    #### Caching detail
    Cached for five minutes.

    ## Routing
    Routes are declared in router.ts.
    Templates render the route model.
    """

  @Test("Splits at headings, names chunks by heading path, ignores code fences")
  func sectionsAndNames() {
    let chunks = MarkdownSectionChunker().chunk(text: document)
    let names = chunks.map(\.constructName)
    #expect(names == ["Introduction", "Design System › Data layer › Requests", "Design System › Routing"])
    #expect(chunks.allSatisfy { $0.constructType == "section" })
    let requests = chunks[1]
    #expect(requests.text.contains("pnpm test"), "a fenced '#' comment is not a heading")
    #expect(requests.text.contains("#### Caching detail"), "a level-4 heading stays inside its section")
    #expect(requests.text.hasPrefix("# Design System"), "empty parent headings fold into the next section")
  }

  @Test("Line numbers are 1-based and cover the document without gaps")
  func lineRanges() {
    let chunks = MarkdownSectionChunker().chunk(text: document)
    let lineCount = document.split(separator: "\n", omittingEmptySubsequences: false).count
    #expect(chunks.first?.startLine == 1)
    #expect(chunks.last?.endLine == lineCount)
    for (a, b) in zip(chunks, chunks.dropFirst()) { #expect(b.startLine == a.endLine + 1) }
  }

  @Test("A long section splits into named parts at blank lines")
  func longSection() {
    let paragraphs = (1...40).map { "Paragraph \($0) line one.\nParagraph \($0) line two.\n" }.joined(separator: "\n")
    let chunks = MarkdownSectionChunker(maxLines: 30).chunk(text: "## Big section\n" + paragraphs)
    #expect(chunks.count > 1)
    #expect(chunks.allSatisfy { $0.constructName?.hasPrefix("Big section (part ") == true })
    #expect(chunks.allSatisfy { $0.endLine - $0.startLine + 1 <= 30 })
  }

  @Test("Only Markdown files take the section signature")
  func perLanguageSignature() {
    let chunker = HybridChunker()
    #expect(chunker.chunkingSignature(forLanguage: "Swift") == chunker.chunkingSignature)
    #expect(chunker.chunkingSignature(forLanguage: nil) == chunker.chunkingSignature)
    #expect(chunker.chunkingSignature(forLanguage: "Markdown") != chunker.chunkingSignature)
    #expect(chunker.chunk(text: "## A\nbody line\nsecond line\n", language: "Markdown").first?.constructName == "A")
  }
}
