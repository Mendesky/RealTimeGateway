import Testing
@testable import RealTimeGateway

final class DeliveryRecorder: @unchecked Sendable {
    var broadcasted: [Int] = []
    var logs: [String] = []
}

@Suite struct DeliveryTests {

    @Test func processorThrowDoesNotBroadcast() async {
        let r = DeliveryRecorder()
        let outcome = await deliverEvent(
            produce: { () throws -> Int? in throw TestSinkError() },
            broadcast: { r.broadcasted.append($0) },
            log: { r.logs.append($0) }
        )
        #expect(outcome == .processorFailed)
        #expect(r.broadcasted.isEmpty)
        #expect(r.logs.count == 1)
    }

    @Test func nilMeansNoMessage() async {
        let r = DeliveryRecorder()
        let outcome = await deliverEvent(
            produce: { () throws -> Int? in nil },
            broadcast: { r.broadcasted.append($0) },
            log: { r.logs.append($0) }
        )
        #expect(outcome == .noMessage)
        #expect(r.broadcasted.isEmpty)
    }

    @Test func broadcastThrowIsReported() async {
        let r = DeliveryRecorder()
        let outcome = await deliverEvent(
            produce: { () throws -> Int? in 1 },
            broadcast: { _ in throw TestSinkError() },
            log: { r.logs.append($0) }
        )
        #expect(outcome == .broadcastFailed)
        #expect(r.logs.count == 1)
    }

    @Test func successIsSent() async {
        let r = DeliveryRecorder()
        let outcome = await deliverEvent(
            produce: { () throws -> Int? in 7 },
            broadcast: { r.broadcasted.append($0) },
            log: { r.logs.append($0) }
        )
        #expect(outcome == .sent)
        #expect(r.broadcasted == [7])
    }

    @Test func sequenceContinuesPastProcessorFailure() async {
        let r = DeliveryRecorder()
        var outcomes: [DeliveryOutcome] = []
        for n in [1, 2, 3] {
            outcomes.append(await deliverEvent(
                produce: { () throws -> Int? in
                    if n == 2 { throw TestSinkError() }
                    return n
                },
                broadcast: { r.broadcasted.append($0) },
                log: { r.logs.append($0) }
            ))
        }
        #expect(outcomes == [.sent, .processorFailed, .sent])
        #expect(r.broadcasted == [1, 3])
    }
}
