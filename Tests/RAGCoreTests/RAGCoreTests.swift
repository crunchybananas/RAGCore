@testable import RAGCore
import Testing

@Suite("RAGCore")
struct RAGCoreTests {
  @Test("VectorMath cosine similarity")
  func cosineSimilarity() {
    let a: [Float] = [1, 0, 0]
    let b: [Float] = [1, 0, 0]
    #expect(VectorMath.cosineSimilarity(a, b) == 1.0)

    let c: [Float] = [0, 1, 0]
    #expect(VectorMath.cosineSimilarity(a, c) == 0.0)
  }

  @Test("VectorMath stableId is deterministic")
  func stableId() {
    let id1 = VectorMath.stableId(for: "hello")
    let id2 = VectorMath.stableId(for: "hello")
    #expect(id1 == id2)
    #expect(!id1.isEmpty)
  }

  @Test("VectorMath encode/decode roundtrip")
  func vectorRoundtrip() {
    let original: [Float] = [1.0, 2.5, -3.14, 0.0]
    let encoded = VectorMath.encodeVector(original)
    let decoded = VectorMath.decodeVector(encoded)
    #expect(decoded.count == original.count)
    for (a, b) in zip(original, decoded) {
      #expect(abs(a - b) < 0.0001)
    }
  }
}
