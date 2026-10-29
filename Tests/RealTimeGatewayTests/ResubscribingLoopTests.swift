import Foundation
import Testing
@testable import RealTimeGateway

/// 腳本化的訂閱來源。測試是單一 task 順序執行，所以用 @unchecked Sendable 的小 class 記錄狀態即可。
final class LoopHarness: @unchecked Sendable {
    enum Step {
        case fail
        /// 送出 events 後正常結束；lifetime 是這個 session 在假時鐘上「活」多久
        case events([Int], lifetime: Duration = .zero)
        case eventsThenFail([Int], lifetime: Duration = .zero)
    }

    let steps: [Step]
    let maxSleeps: Int
    var index = 0
    var sleeps: [Duration] = []
    var handled: [Int] = []
    var resubscribedCount = 0
    var logs: [String] = []
    var offset: Duration = .zero
    let base = ContinuousClock.now

    init(steps: [Step], maxSleeps: Int) {
        self.steps = steps
        self.maxSleeps = maxSleeps
    }

    func run() async {
        let loop = ResubscribingLoop<Int>(
            policy: .default,
            now: { self.base + self.offset },
            sleep: { d in
                self.sleeps.append(d)
                if self.sleeps.count >= self.maxSleeps { throw CancellationError() }
            }
        )
        await loop.run(
            subscribe: {
                defer { self.index += 1 }
                guard self.index < self.steps.count else { throw TestSinkError() }
                switch self.steps[self.index] {
                case .fail:
                    throw TestSinkError()
                case .events(let items, let lifetime):
                    self.offset += lifetime
                    return Self.stream(items, failing: false)
                case .eventsThenFail(let items, let lifetime):
                    self.offset += lifetime
                    return Self.stream(items, failing: true)
                }
            },
            handle: { self.handled.append($0) },
            onResubscribed: { self.resubscribedCount += 1 },
            log: { self.logs.append($0) }
        )
    }

    static func stream(_ items: [Int], failing: Bool) -> AsyncThrowingStream<Int, any Error> {
        AsyncThrowingStream { c in
            for i in items { c.yield(i) }
            c.finish(throwing: failing ? TestSinkError() : nil)
        }
    }
}

@Suite struct ResubscribingLoopTests {

    @Test func backoffGrowsWhileFailing() async {
        let h = LoopHarness(steps: [], maxSleeps: 4)
        await h.run()
        #expect(h.sleeps == [.seconds(1), .seconds(2), .seconds(4), .seconds(8)])
    }

    @Test func backoffIsCappedAndNeverGivesUp() async {
        let h = LoopHarness(steps: [], maxSleeps: 12)
        await h.run()
        #expect(h.sleeps.count == 12)  // 遠超原本的 10 次上限仍持續重試
        let s = [1, 2, 4, 8, 16].map { Duration.seconds($0) } + Array(repeating: Duration.seconds(30), count: 7)
        #expect(h.sleeps == s)
        #expect(h.index == 12)
    }

    @Test func counterResetsAfterSessionThatReceivedEvents() async {
        let h = LoopHarness(steps: [.fail, .fail, .events([1]), .fail], maxSleeps: 4)
        await h.run()
        #expect(h.sleeps == [.seconds(1), .seconds(2), .seconds(1), .seconds(2)])
    }

    @Test func counterResetsAfterEventsThenError() async {
        let h = LoopHarness(steps: [.fail, .fail, .eventsThenFail([1, 2]), .fail], maxSleeps: 4)
        await h.run()
        #expect(h.sleeps == [.seconds(1), .seconds(2), .seconds(1), .seconds(2)])
    }

    @Test func counterResetsAfterStableSessionWithoutEvents() async {
        let h = LoopHarness(steps: [.fail, .fail, .events([], lifetime: .seconds(60))], maxSleeps: 3)
        await h.run()
        #expect(h.sleeps == [.seconds(1), .seconds(2), .seconds(1)])
    }

    @Test func counterNotResetByShortSessionWithoutEvents() async {
        let h = LoopHarness(steps: [.fail, .fail, .events([], lifetime: .seconds(59))], maxSleeps: 3)
        await h.run()
        #expect(h.sleeps == [.seconds(1), .seconds(2), .seconds(4)])
    }

    @Test func normalEndOfStreamResubscribes() async {
        let h = LoopHarness(steps: [.events([1, 2]), .events([3])], maxSleeps: 2)
        await h.run()
        #expect(h.index == 2)
        #expect(h.handled == [1, 2, 3])
        #expect(h.logs.contains { $0.contains("ended normally") })
    }

    @Test func onResubscribedOnlyAfterFirstSuccessfulSubscription() async {
        // 第一次嘗試失敗，第二次才首次成功：不算「重訂閱」
        let h = LoopHarness(steps: [.fail, .events([1])], maxSleeps: 2)
        await h.run()
        #expect(h.resubscribedCount == 0)

        let h2 = LoopHarness(steps: [.fail, .events([1]), .events([2]), .events([3])], maxSleeps: 4)
        await h2.run()
        #expect(h2.resubscribedCount == 2)
        #expect(h2.handled == [1, 2, 3])
    }

    @Test func handleReceivesEventsInOrder() async {
        let h = LoopHarness(steps: [.eventsThenFail([1, 2, 3]), .events([4, 5])], maxSleeps: 2)
        await h.run()
        #expect(h.handled == [1, 2, 3, 4, 5])
    }
}
