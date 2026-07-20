@testable import RAGCore
import Testing

/// Batching rules for `enrichEmbeddings`' embed requests.
///
/// The bug these guard: batching by a fixed count of 32 ignored payload size,
/// so a repo whose chunks averaged ~5 KB produced 161 KB requests that killed
/// ollama's embedding runner (EOF, not a clean error). The throw escaped the
/// whole pass and the bare-LIMIT select re-fetched the identical batch every
/// run — a permanent head-of-line block at 5485/6484 chunks.
@Suite("Enrich batching")
struct EnrichBatchingTests {
  private func text(bytes: Int) -> String { String(repeating: "x", count: bytes) }

  @Test("Batches stay under the byte cap")
  func respectsByteCap() {
    let texts = Array(repeating: text(bytes: 5_000), count: 32)
    let ranges = RAGStore.embedBatchRanges(for: texts)

    #expect(ranges.count > 1, "32 × 5 KB must not go out as one 160 KB request")
    for range in ranges {
      let bytes = texts[range].reduce(0) { $0 + $1.utf8.count }
      #expect(bytes <= RAGStore.maxEmbedBatchBytes)
    }
  }

  @Test("Count cap still applies to small chunks")
  func respectsCountCap() {
    let texts = Array(repeating: text(bytes: 10), count: 200)
    let ranges = RAGStore.embedBatchRanges(for: texts)

    #expect(ranges.allSatisfy { $0.count <= RAGStore.maxEmbedBatchCount })
    // Ceiling, not floor — the trailing partial batch counts.
    let expected = (200 + RAGStore.maxEmbedBatchCount - 1) / RAGStore.maxEmbedBatchCount
    #expect(ranges.count == expected)
    #expect(ranges.reduce(0) { $0 + $1.count } == 200)
  }

  @Test("Every chunk is covered exactly once, in order")
  func coversAllInputsContiguously() {
    let texts = (0..<77).map { text(bytes: $0 * 700) }
    let ranges = RAGStore.embedBatchRanges(for: texts)

    #expect(ranges.first?.lowerBound == 0)
    #expect(ranges.last?.upperBound == texts.count)
    for (previous, next) in zip(ranges, ranges.dropFirst()) {
      #expect(previous.upperBound == next.lowerBound, "ranges must not overlap or skip")
    }
    #expect(ranges.reduce(0) { $0 + $1.count } == texts.count)
  }

  @Test("An oversized chunk is isolated, never dropped")
  func oversizedChunkGetsItsOwnBatch() {
    let huge = text(bytes: RAGStore.maxEmbedBatchBytes * 3)
    let texts = [text(bytes: 100), huge, text(bytes: 100)]
    let ranges = RAGStore.embedBatchRanges(for: texts)

    #expect(ranges.reduce(0) { $0 + $1.count } == 3, "no chunk may be dropped")
    let hugeRange = ranges.first { $0.contains(1) }
    #expect(hugeRange?.count == 1, "an over-cap chunk must travel alone")
  }

  @Test("Empty input produces no requests")
  func emptyInput() {
    #expect(RAGStore.embedBatchRanges(for: []).isEmpty)
  }

  @Test("A single chunk is one batch")
  func singleChunk() {
    #expect(RAGStore.embedBatchRanges(for: [text(bytes: 42)]) == [0..<1])
  }

  /// The concrete production regression, in numbers: the first 32 unenriched
  /// tio-api chunks totalled 161,767 bytes and reliably killed the runner.
  @Test("The tio-api batch that killed the runner is now split up")
  func realWorldFailingBatchIsSplit() {
    let texts = Array(repeating: text(bytes: 161_767 / 32), count: 32)
    let ranges = RAGStore.embedBatchRanges(for: texts)

    let largest = ranges.map { texts[$0].reduce(0) { $0 + $1.utf8.count } }.max() ?? 0
    #expect(largest <= RAGStore.maxEmbedBatchBytes)
    #expect(largest < 40_000, "must stay clear of the observed failure threshold")
  }
}
