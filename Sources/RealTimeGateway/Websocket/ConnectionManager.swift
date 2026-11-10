//
//  ConnectionManager.swift
//  RealTimeGateway
//
//  Created by 蔡東棋 on 2025/11/27.
//

import HummingbirdWebSocket
import Foundation

enum BroadcastError: Error {
    case jsonStringDecodingFailed
}

struct SubscriptionRegistry {
    var subscriptions: [Subscription] = []
    
    mutating func register(subscription: Subscription) {
        self.subscriptions.append(subscription)
    }
    
    mutating func subscribe(topic: String, id: UUID) {
        if let index = self.subscriptions.firstIndex { $0.topic == topic } {
            self.subscriptions[index].ids.insert(id)
        } else {
            self.subscriptions.append(.init(topic: topic, ids: .init([id])))
        }
    }
    
    mutating func deregister(topic: String) {
        self.subscriptions.removeAll { $0.topic == topic }
    }
    
    mutating func unsubscribe(id: UUID) {
        self.subscriptions = self.subscriptions.compactMap { subscription in
            var ids = subscription.ids
            ids.remove(id)
            return ids.isEmpty ? nil : Subscription(topic: subscription.topic, ids: ids)
        }
    }
    
    func findSubscription(topic: String) -> Subscription? {
        return self.subscriptions.first(where: { $0.topic == topic })
    }
}

struct Subscription {
    var topic: String
    var ids: Set<UUID>
}
    
package struct BroadcastResult: Sendable, Equatable {
    package var delivered: Int
    package var removed: [UUID]
}

package actor ConnectionManager {
    
    var connections: [UUID: any MessageSink] = [:]
    var subscriptionRegistry: SubscriptionRegistry = .init()
    
    func add(id: UUID, writer: WebSocketOutboundWriter) {
        add(id: id, sink: writer)
    }

    func add(id: UUID, sink: any MessageSink) {
        connections[id] = sink
    }
    
    func remove(id: UUID) {
        connections.removeValue(forKey: id)
        self.subscriptionRegistry.unsubscribe(id: id)
    }
    
    func subscribe(to topic: String, id: UUID) {
        self.subscriptionRegistry.subscribe(topic: topic, id: id)
    }
    
    func unsubscribe(id: UUID) {
        self.subscriptionRegistry.unsubscribe(id: id)
    }
    
    @discardableResult
    func broadcast<T: WebSocketMessage>(message: T, to topic: String? = nil) async throws -> BroadcastResult {
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        let jsonData = try encoder.encode(message)
        
        guard let jsonString = String(data: jsonData, encoding: .utf8) else {
            throw BroadcastError.jsonStringDecodingFailed
        }
        
        let targetIds: Set<UUID>
        if let topic = topic {
            if let subscription = self.subscriptionRegistry.findSubscription(topic: topic) {
                targetIds = subscription.ids
            } else {
                targetIds = Set()
            }
        } else {
            targetIds = Set(connections.keys)
        }
        
        let targets: [(UUID, any MessageSink)] = targetIds.compactMap { id in
            connections[id].map { (id, $0) }
        }
        let failed: [UUID] = await withTaskGroup(of: UUID?.self) { group in
            for (id, sink) in targets {
                group.addTask {
                    do {
                        try await sink.send(text: jsonString)
                        return nil
                    } catch {
                        logger.warning("broadcast write failed, dropping connection \(id): \(error)")
                        return id
                    }
                }
            }
            var failed: [UUID] = []
            for await id in group {
                if let id { failed.append(id) }
            }
            return failed
        }
        for id in failed { remove(id: id) }
        return BroadcastResult(delivered: targets.count - failed.count, removed: failed)
    }
}
