import Foundation
@testable import RealTimeGateway

struct TestPayload: WebSocketPayload, Equatable {
    var n: Int
}

typealias TestMessage = GatewayMessage<TestPayload>

struct TestSinkError: Error {}

/// 假連線：記錄收到的字串；mode 決定成功、失敗或延遲後成功。
actor FakeSink: MessageSink {
    enum Mode: Sendable { case ok, fail, slow(Duration) }

    let mode: Mode
    private(set) var received: [String] = []

    init(_ mode: Mode = .ok) { self.mode = mode }

    func send(text: String) async throws {
        switch mode {
        case .ok:
            received.append(text)
        case .fail:
            throw TestSinkError()
        case .slow(let d):
            try await Task.sleep(for: d)
            received.append(text)
        }
    }
}

func waitUntil(timeout: Duration = .seconds(5), _ condition: () async -> Bool) async -> Bool {
    let deadline = ContinuousClock.now + timeout
    while ContinuousClock.now < deadline {
        if await condition() { return true }
        try? await Task.sleep(for: .milliseconds(10))
    }
    return await condition()
}
