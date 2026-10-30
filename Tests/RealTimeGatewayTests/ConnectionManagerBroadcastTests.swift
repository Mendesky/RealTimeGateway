import Foundation
import Testing
@testable import RealTimeGateway

@Suite struct ConnectionManagerBroadcastTests {

    @Test func failedConnectionDoesNotBlockOthersAndIsRemoved() async throws {
        let cm = ConnectionManager()
        let good1 = FakeSink(.ok), bad = FakeSink(.fail), slowGood = FakeSink(.slow(.milliseconds(50)))
        let id1 = UUID(), idBad = UUID(), id3 = UUID()
        await cm.add(id: id1, sink: good1)
        await cm.add(id: idBad, sink: bad)
        await cm.add(id: id3, sink: slowGood)

        let result = try await cm.broadcast(message: TestMessage(type: "t", payload: .init(n: 1)))

        #expect(result.delivered == 2)
        #expect(result.removed == [idBad])
        let r1 = await good1.received
        let r3 = await slowGood.received
        #expect(r1.count == 1)
        #expect(r1 == r3)
        #expect(r1.first?.contains("\"type\":\"t\"") == true)
        let ids = await cm.connections.keys
        #expect(Set(ids) == [id1, id3])
    }

    @Test func allSucceed() async throws {
        let cm = ConnectionManager()
        let sinks = [FakeSink(), FakeSink(), FakeSink()]
        for s in sinks { await cm.add(id: UUID(), sink: s) }
        let result = try await cm.broadcast(message: TestMessage(type: "t", payload: .init(n: 1)))
        #expect(result.delivered == 3)
        #expect(result.removed.isEmpty)
        for s in sinks { #expect(await s.received.count == 1) }
    }

    @Test func noConnections() async throws {
        let cm = ConnectionManager()
        let result = try await cm.broadcast(message: TestMessage(type: "t", payload: .init(n: 1)))
        #expect(result == BroadcastResult(delivered: 0, removed: []))
    }

    @Test func topicOnlyReachesSubscribers() async throws {
        let cm = ConnectionManager()
        let a = FakeSink(), b = FakeSink()
        let idA = UUID(), idB = UUID()
        await cm.add(id: idA, sink: a)
        await cm.add(id: idB, sink: b)
        await cm.subscribe(to: "topic", id: idA)

        let result = try await cm.broadcast(message: TestMessage(type: "t", payload: .init(n: 1)), to: "topic")

        #expect(result.delivered == 1)
        #expect(await a.received.count == 1)
        #expect(await b.received.isEmpty)
    }

    @Test func removingAlreadyRemovedConnectionIsSafe() async throws {
        let cm = ConnectionManager()
        let idBad = UUID()
        await cm.add(id: idBad, sink: FakeSink(.fail))
        let result = try await cm.broadcast(message: TestMessage(type: "t", payload: .init(n: 1)))
        #expect(result.removed == [idBad])
        await cm.remove(id: idBad)
        await cm.remove(id: idBad)
        #expect(await cm.connections.isEmpty)
    }
}
