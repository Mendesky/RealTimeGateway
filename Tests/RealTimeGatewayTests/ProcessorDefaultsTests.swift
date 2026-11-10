import Testing
import KurrentDB
import KurrentDB_V1
@testable import RealTimeGateway

/// 只實作 process 的 conformer（模擬 QC 的 QuotingEventMessageProcessor）：不需修改即可編譯，且預設沒有 resync 訊息。
struct LegacyProcessor: EventMessageProcessor {
    typealias MessageType = TestMessage
    func process(eventData: RecordedEvent) throws -> TestMessage? { nil }
}

@Suite struct ProcessorDefaultsTests {
    @Test func resubscribedMessageDefaultsToNil() {
        #expect(LegacyProcessor().resubscribedMessage(streamName: "any") == nil)
        #expect(StandardEventMessageProcessor().resubscribedMessage(streamName: "any") == nil)
    }
}
