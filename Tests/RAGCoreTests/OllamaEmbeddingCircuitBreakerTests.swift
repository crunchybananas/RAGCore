import XCTest
@testable import RAGCore

final class OllamaEmbeddingCircuitBreakerTests: XCTestCase {
  func testRequestRangesRespectByteAndCountCaps() {
    let texts = [
      String(repeating: "a", count: 8_000),
      String(repeating: "b", count: 8_000),
      "c",
      "d",
      "e",
      "f",
      "g",
    ]

    XCTAssertEqual(
      OllamaEmbeddingProvider.requestRanges(for: texts),
      [0..<2, 2..<6, 6..<7]
    )
  }

  func testRequestRangesKeepOversizedSingleText() {
    let texts = [String(repeating: "a", count: 17_000), "b"]
    XCTAssertEqual(
      OllamaEmbeddingProvider.requestRanges(for: texts),
      [0..<1, 1..<2]
    )
  }

  func testPreparationMatchesProviderContextLimit() {
    let prepared = OllamaEmbeddingProvider.prepare(
      texts: [String(repeating: "a", count: 9_000)],
      model: "qwen3-embedding:0.6b"
    )
    XCTAssertEqual(prepared.first?.count, 8_000)
  }

  func testCircuitOpensAfterThreeTransientFailuresAndBlocksRetries() async {
    let gate = OllamaEmbeddingRequestGate(
      failureThreshold: 3,
      initialCooldown: 60,
      maximumCooldown: 300
    )
    let startedAt = Date(timeIntervalSince1970: 1_000)

    for attempt in 1...2 {
      do {
        let _: Int = try await gate.perform(
          endpoint: "http://localhost:11434",
          model: "qwen3-embedding:0.6b",
          now: startedAt
        ) {
          throw URLError(.networkConnectionLost)
        }
        XCTFail("Attempt \(attempt) should fail")
      } catch {
        XCTAssertFalse(error is OllamaEmbeddingCircuitOpenError)
      }
    }

    do {
      let _: Int = try await gate.perform(
        endpoint: "http://localhost:11434",
        model: "qwen3-embedding:0.6b",
        now: startedAt
      ) {
        throw URLError(.networkConnectionLost)
      }
      XCTFail("The third transient failure should open the circuit")
    } catch let error as OllamaEmbeddingCircuitOpenError {
      XCTAssertTrue(error.openedNow)
      XCTAssertEqual(error.consecutiveFailures, 3)
      XCTAssertEqual(error.retryAfter, startedAt.addingTimeInterval(60))
    } catch {
      XCTFail("Unexpected error: \(error)")
    }

    let invocation = InvocationCounter()
    do {
      let _: Int = try await gate.perform(
        endpoint: "http://localhost:11434",
        model: "qwen3-embedding:0.6b",
        now: startedAt.addingTimeInterval(30)
      ) {
        await invocation.increment()
        return 1
      }
      XCTFail("An open circuit should reject without invoking the request")
    } catch let error as OllamaEmbeddingCircuitOpenError {
      XCTAssertFalse(error.openedNow)
    } catch {
      XCTFail("Unexpected error: \(error)")
    }
    let invocationCount = await invocation.value
    XCTAssertEqual(invocationCount, 0)
  }

  func testSuccessfulHalfOpenProbeClosesCircuit() async throws {
    let gate = OllamaEmbeddingRequestGate(
      failureThreshold: 1,
      initialCooldown: 10,
      maximumCooldown: 60
    )
    let startedAt = Date(timeIntervalSince1970: 2_000)

    do {
      let _: Int = try await gate.perform(
        endpoint: "http://localhost:11434",
        model: "qwen3-embedding:0.6b",
        now: startedAt
      ) {
        throw URLError(.timedOut)
      }
      XCTFail("First failure should open a threshold-one circuit")
    } catch is OllamaEmbeddingCircuitOpenError {
      // Expected.
    }

    let recovered: Int = try await gate.perform(
      endpoint: "http://localhost:11434",
      model: "qwen3-embedding:0.6b",
      now: startedAt.addingTimeInterval(11)
    ) {
      42
    }
    XCTAssertEqual(recovered, 42)

    do {
      let _: Int = try await gate.perform(
        endpoint: "http://localhost:11434",
        model: "qwen3-embedding:0.6b",
        now: startedAt.addingTimeInterval(12)
      ) {
        throw URLError(.timedOut)
      }
      XCTFail("Threshold-one circuit should open again after recovery")
    } catch let error as OllamaEmbeddingCircuitOpenError {
      XCTAssertEqual(error.retryAfter, startedAt.addingTimeInterval(22))
    }
  }

  func testSameEndpointRequestsAreSerialized() async throws {
    let gate = OllamaEmbeddingRequestGate()
    let activity = RequestActivity()

    async let first: Int = gate.perform(
      endpoint: "http://localhost:11434",
      model: "qwen3-embedding:0.6b"
    ) {
      await activity.begin()
      try await Task.sleep(for: .milliseconds(30))
      await activity.end()
      return 1
    }
    async let second: Int = gate.perform(
      endpoint: "http://localhost:11434",
      model: "qwen3-embedding:0.6b"
    ) {
      await activity.begin()
      try await Task.sleep(for: .milliseconds(30))
      await activity.end()
      return 2
    }

    _ = try await (first, second)
    let maximumConcurrency = await activity.maximum
    XCTAssertEqual(maximumConcurrency, 1)
  }
}

private actor InvocationCounter {
  private(set) var value = 0
  func increment() { value += 1 }
}

private actor RequestActivity {
  private var current = 0
  private(set) var maximum = 0

  func begin() {
    current += 1
    maximum = max(maximum, current)
  }

  func end() {
    current -= 1
  }
}
