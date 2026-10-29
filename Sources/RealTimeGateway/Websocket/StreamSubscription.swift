import KurrentDB
import KurrentDB_V1

/// 訂閱選項：解析 link，並明確從 `.end` 開始（重訂閱不續接舊位置，漏掉的區間由 resync 補）。
func streamSubscriptionOptions(_ options: StreamsSubscribeOptions) -> StreamsSubscribeOptions {
    options.resolveLinks().startFrom(revision: .end)
}

/// 每個 streamName 只保留一個迴圈 Task：重複訂閱同一條 stream 時，先取消舊的再換成新的，避免重複推送。
struct StreamTaskRegistry {
    private var tasks: [String: Task<Void, Never>] = [:]

    var count: Int { tasks.count }

    mutating func start(streamName: String, _ make: () -> Task<Void, Never>) {
        tasks[streamName]?.cancel()
        tasks[streamName] = make()
    }
}

/// 單一 stream 的接線：重訂閱迴圈＋逐事件處理＋重訂閱後的 resync。與 KurrentDB 無關，方便測試。
nonisolated(nonsending) func runStreamSubscription<Element, M>(
    streamName: String,
    loop: ResubscribingLoop<Element>,
    subscribe: nonisolated(nonsending) () async throws -> AsyncThrowingStream<Element, any Error>,
    process: (Element) throws -> M?,
    resubscribedMessage: () -> M?,
    broadcast: nonisolated(nonsending) (M) async throws -> Void
) async {
    await loop.run(
        subscribe: subscribe,
        handle: { event in
            _ = await deliverEvent(
                produce: { try process(event) },
                broadcast: broadcast,
                log: { logger.error("\(streamName): \($0)") },
                debug: { logger.debug("\(streamName): \($0)") }
            )
        },
        onResubscribed: {
            if let message = resubscribedMessage() {
                _ = try? await broadcast(message)
            }
        },
        log: { logger.warning("\(streamName): \($0)") }
    )
}
