import Foundation
import Hummingbird
import HummingbirdTesting
import HummingbirdWebSocket
import HummingbirdWSTesting
import Testing
@testable import RealTimeGateway

/// 行程內 loopback（127.0.0.1、port 0）：不連 KurrentDB，也不連外部網路。
actor Collector {
    private(set) var items: [String] = []
    func add(_ s: String) { items.append(s) }
}

actor Gate {
    private(set) var isOpen = false
    func open() { isOpen = true }
}

@Suite struct WebSocketLoopbackTests {

    func makeApp(_ cm: ConnectionManager) -> some ApplicationProtocol {
        Application(
            router: Router(),
            server: .http1WebSocketUpgrade { _, _, _ in
                .upgrade([:]) { inbound, outbound, _ in
                    let id = UUID()
                    await cm.add(id: id, writer: outbound)
                    do {
                        for try await _ in inbound.messages(maxSize: .max) {}
                    } catch {}
                    await cm.remove(id: id)
                }
            },
            configuration: .init(address: .hostname("127.0.0.1", port: 0))
        )
    }

    @Test func twoClientsReceiveSameBroadcast() async throws {
        let cm = ConnectionManager()
        let collector = Collector()
        let app = makeApp(cm)
        try await app.test(.live) { client in
            try await withThrowingTaskGroup(of: Void.self) { group in
                for _ in 0..<2 {
                    group.addTask {
                        try await client.ws("/") { inbound, _, _ in
                            var it = inbound.messages(maxSize: .max).makeAsyncIterator()
                            if case .text(let s)? = try await it.next() { await collector.add(s) }
                        }
                    }
                }
                #expect(await waitUntil { await cm.connections.count == 2 })
                try await cm.broadcast(message: TestMessage(type: "ping", payload: .init(n: 1)))
                try await group.waitForAll()
            }
        }
        let items = await collector.items
        #expect(items.count == 2)
        #expect(items[0] == items[1])
        #expect(items[0].contains("\"type\":\"ping\""))
    }

    @Test func closedClientIsDroppedAndOthersKeepReceiving() async throws {
        let cm = ConnectionManager()
        let collector = Collector()
        let gate = Gate()
        let app = makeApp(cm)
        try await app.test(.live) { client in
            try await withThrowingTaskGroup(of: Void.self) { group in
                // A：連上就關閉
                group.addTask {
                    try await client.ws("/") { _, _, _ in
                        _ = await waitUntil { await gate.isOpen }
                    }
                }
                // B：等一則訊息
                group.addTask {
                    try await client.ws("/") { inbound, _, _ in
                        var it = inbound.messages(maxSize: .max).makeAsyncIterator()
                        if case .text(let s)? = try await it.next() { await collector.add(s) }
                    }
                }
                #expect(await waitUntil { await cm.connections.count == 2 })
                await gate.open()
                // 等 A 被伺服器端移除（A 的 handler 回傳後連線關閉）
                #expect(await waitUntil { await cm.connections.count == 1 })
                try await cm.broadcast(message: TestMessage(type: "after-close", payload: .init(n: 2)))
                try await group.waitForAll()
            }
            #expect(await cm.connections.count <= 1)
        }
        let items = await collector.items
        #expect(items.count == 1)
        #expect(items[0].contains("after-close"))
    }
}
