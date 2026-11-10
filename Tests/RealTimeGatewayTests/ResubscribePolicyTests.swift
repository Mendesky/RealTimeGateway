import Testing
@testable import RealTimeGateway

@Suite struct ResubscribePolicyTests {
    let policy = ResubscribePolicy.default

    @Test func defaults() {
        #expect(policy.initialDelay == .seconds(1))
        #expect(policy.multiplier == 2)
        #expect(policy.maxDelay == .seconds(30))
        #expect(policy.stableAfter == .seconds(60))
    }

    @Test func delayGrowsExponentially() {
        #expect(policy.delay(forAttempt: 0) == .seconds(1))
        #expect(policy.delay(forAttempt: 1) == .seconds(2))
        #expect(policy.delay(forAttempt: 4) == .seconds(16))
    }

    @Test func delayIsCapped() {
        #expect(policy.delay(forAttempt: 5) == .seconds(30))
        #expect(policy.delay(forAttempt: 6) == .seconds(30))
    }

    @Test func hugeAttemptDoesNotOverflow() {
        #expect(policy.delay(forAttempt: 100) == .seconds(30))
        #expect(policy.delay(forAttempt: Int.max) == .seconds(30))
    }
}
