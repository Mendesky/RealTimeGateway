import HummingbirdWebSocket

/// 一條可寫出文字訊息的連線。抽出來是為了讓測試用假的連線。
package protocol MessageSink: Sendable {
    func send(text: String) async throws
}

extension WebSocketOutboundWriter: MessageSink {
    package func send(text: String) async throws {
        try await self.write(.text(text))
    }
}
