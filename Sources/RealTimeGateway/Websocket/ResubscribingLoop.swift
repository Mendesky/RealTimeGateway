import Foundation

/// 重訂閱策略。退避：initialDelay × multiplier^attempt，上限 maxDelay；永不放棄。
/// 歸零條件：上一次訂閱「收到 ≥1 個事件」或「連續存活 ≥ stableAfter」。
public struct ResubscribePolicy: Sendable, Equatable {
    public var initialDelay: Duration
    public var multiplier: Int
    public var maxDelay: Duration
    public var stableAfter: Duration

    public init(initialDelay: Duration = .seconds(1), multiplier: Int = 2,
                maxDelay: Duration = .seconds(30), stableAfter: Duration = .seconds(60)) {
        // 避免 0 或負值造成熱迴圈
        self.initialDelay = max(initialDelay, .milliseconds(1))
        self.multiplier = max(multiplier, 1)
        self.maxDelay = maxDelay
        self.stableAfter = stableAfter
    }

    public static let `default` = ResubscribePolicy()

    /// attempt 0 → initialDelay、1 → ×multiplier …，封頂 maxDelay；不會溢位。
    func delay(forAttempt attempt: Int) -> Duration {
        var delay = initialDelay
        if delay >= maxDelay { return maxDelay }
        let factor = max(multiplier, 1)
        var i = 0
        while i < attempt {
            if factor == 1 { return delay }
            // 先比較再乘，避免溢位
            if delay >= maxDelay / factor { return maxDelay }
            delay = delay * factor
            i += 1
        }
        return min(delay, maxDelay)
    }
}

/// 與 KurrentDB 無關的重訂閱迴圈，方便測試（注入 now／sleep）。
struct ResubscribingLoop<Element> {
    var policy: ResubscribePolicy
    var now: () -> ContinuousClock.Instant = { .now }
    var sleep: (Duration) async throws -> Void = { try await Task.sleep(for: $0) }

    nonisolated(nonsending) func run(
        subscribe: nonisolated(nonsending) () async throws -> AsyncThrowingStream<Element, any Error>,
        handle: nonisolated(nonsending) (Element) async -> Void,
        onResubscribed: nonisolated(nonsending) () async -> Void,
        log: (String) -> Void
    ) async {
        var attempt = 0
        var hadSession = false
        while !Task.isCancelled {
            let startedAt = now()
            var receivedAny = false
            do {
                let events = try await subscribe()
                if hadSession { await onResubscribed() }
                hadSession = true
                for try await e in events {
                    receivedAny = true
                    await handle(e)
                }
                log("stream ended normally, resubscribing")
            } catch is CancellationError {
                return
            } catch {
                log("subscription error: \(error)")
            }
            if Task.isCancelled { return }
            if receivedAny || now() - startedAt >= policy.stableAfter { attempt = 0 }
            let d = policy.delay(forAttempt: attempt)
            attempt += 1
            do { try await sleep(d) } catch { return }
        }
    }
}

enum DeliveryOutcome: Equatable { case sent, noMessage, processorFailed, broadcastFailed }

nonisolated(nonsending) func deliverEvent<M>(
    produce: () throws -> M?,
    broadcast: nonisolated(nonsending) (M) async throws -> Void,
    log: (String) -> Void,
    debug: (String) -> Void = { _ in }
) async -> DeliveryOutcome {
    let message: M?
    do { message = try produce() } catch {
        log("processor failed, skipping event: \(error)")
        return .processorFailed
    }
    guard let message else {
        debug("event has no corresponding message")
        return .noMessage
    }
    do { try await broadcast(message) } catch {
        log("broadcast failed, skipping event: \(error)")
        return .broadcastFailed
    }
    return .sent
}
