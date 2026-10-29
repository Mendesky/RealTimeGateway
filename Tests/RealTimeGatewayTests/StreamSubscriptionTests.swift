import Foundation
import Testing
import KurrentDB_V1
@testable import RealTimeGateway

final class WiringRecorder: @unchecked Sendable {
    var broadcasted: [String] = []
}

@Suite struct StreamSubscriptionTests {

    // W2：重訂閱起點
    @Test func resubscribeStartsFromEndNotStart() {
        let options = streamSubscriptionOptions(StreamsSubscribeOptions())
        guard case .end = options.revision else {
            Issue.record("expected .end, got \(options.revision)")
            return
        }
        #expect(options.resolveLinksEnabled)
    }

    // W2：事件被處理與廣播，重訂閱成功後才送 resync（首次成功不送）
    @Test func resyncSentOnlyAfterResubscription() async {
        let h = LoopHarness(steps: [.fail, .events([1, 2]), .events([3])], maxSleeps: 3)
        let r = WiringRecorder()
        let loop = ResubscribingLoop<Int>(
            policy: .default,
            now: { h.base + h.offset },
            sleep: { d in
                h.sleeps.append(d)
                if h.sleeps.count >= h.maxSleeps { throw CancellationError() }
            }
        )
        await runStreamSubscription(
            streamName: "s",
            loop: loop,
            subscribe: {
                defer { h.index += 1 }
                guard h.index < h.steps.count else { throw TestSinkError() }
                switch h.steps[h.index] {
                case .fail: throw TestSinkError()
                case .events(let items, _), .eventsThenFail(let items, _): return LoopHarness.stream(items, failing: false)
                }
            },
            process: { (e: Int) throws -> String? in e == 2 ? nil : "m\(e)" },
            resubscribedMessage: { "resync" },
            broadcast: { r.broadcasted.append($0) }
        )
        // 第 2 次嘗試是首次成功（無 resync）；第 3 次是重訂閱 → resync 在其事件之前
        #expect(r.broadcasted == ["m1", "resync", "m3"])
    }

    @Test func noResyncWhenProcessorHasNoMessage() async {
        let h = LoopHarness(steps: [.events([1]), .events([2])], maxSleeps: 2)
        let r = WiringRecorder()
        let loop = ResubscribingLoop<Int>(policy: .default, now: { h.base }, sleep: { _ in
            h.sleeps.append(.zero)
            if h.sleeps.count >= 2 { throw CancellationError() }
        })
        await runStreamSubscription(
            streamName: "s", loop: loop,
            subscribe: {
                defer { h.index += 1 }
                guard h.index < h.steps.count else { throw TestSinkError() }
                if case .events(let items, _) = h.steps[h.index] { return LoopHarness.stream(items, failing: false) }
                throw TestSinkError()
            },
            process: { (e: Int) throws -> String? in "m\(e)" },
            resubscribedMessage: { nil },
            broadcast: { r.broadcasted.append($0) }
        )
        #expect(r.broadcasted == ["m1", "m2"])
    }

    // W3：取消會讓迴圈結束
    @Test func cancellingTaskStopsLoop() async {
        let task = Task {
            await runStreamSubscription(
                streamName: "s",
                loop: ResubscribingLoop<Int>(policy: ResubscribePolicy(initialDelay: .milliseconds(5), maxDelay: .milliseconds(5))),
                subscribe: { throw TestSinkError() },
                process: { (e: Int) throws -> String? in "m\(e)" },
                resubscribedMessage: { nil },
                broadcast: { _ in }
            )
        }
        try? await Task.sleep(for: .milliseconds(30))
        task.cancel()
        let done = Gate()
        Task { await task.value; await done.open() }
        #expect(await waitUntil(timeout: .seconds(3)) { await done.isOpen })  // 迴圈真的結束
    }

    // W3：同一 streamName 重複訂閱 → 取消舊的，只留新的
    @Test func registryReplacesAndCancelsPreviousTaskForSameStream() async {
        var registry = StreamTaskRegistry()
        let make: () -> Task<Void, Never> = {
            Task { while !Task.isCancelled { try? await Task.sleep(for: .milliseconds(5)) } }
        }
        let first = make()
        registry.start(streamName: "a") { first }
        #expect(!first.isCancelled)
        let second = make()
        registry.start(streamName: "a") { second }
        let other = make()
        registry.start(streamName: "b") { other }

        #expect(first.isCancelled)
        #expect(!second.isCancelled)
        #expect(!other.isCancelled)
        #expect(registry.count == 2)
        let done = Gate()
        Task { await first.value; await done.open() }
        #expect(await waitUntil(timeout: .seconds(3)) { await done.isOpen })  // 舊迴圈已結束
        second.cancel(); other.cancel()
    }

    @Test func policyClampsNonPositiveInputs() {
        let p = ResubscribePolicy(initialDelay: .zero, multiplier: 0)
        #expect(p.initialDelay > .zero)
        #expect(p.multiplier == 1)
        #expect(p.delay(forAttempt: 1_000_000) <= p.maxDelay)
    }
}
