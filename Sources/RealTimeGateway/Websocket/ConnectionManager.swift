//
//  ConnectionManager.swift
//  RealTimeGateway
//
//  Created by 蔡東棋 on 2025/11/27.
//

import HummingbirdWebSocket
import Foundation

enum BroadcastError: Error {
    case jsonEncodingFailed
}

struct SubscriptionRegister {
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
        for subscription in self.subscriptions {
            var newSubscriptions = subscription.ids
            newSubscriptions.remove(id)
            if newSubscriptions.isEmpty {
                self.subscriptions.removeAll { $0.topic == subscription.topic }
            }
            else {
                guard let index = self.subscriptions.firstIndex(where: { $0.topic == subscription.topic }) else {
                    continue
                }
                self.subscriptions[index].ids.remove(id)
                
            }
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
    
package actor ConnectionManager {
    
    var connections: [UUID: WebSocketOutboundWriter] = [:]
    var subscriptionRegister: SubscriptionRegister = .init()
    
    func add(id: UUID, writer: WebSocketOutboundWriter) {
        connections[id] = writer
    }
    
    func remove(id: UUID) {
        connections.removeValue(forKey: id)
        self.subscriptionRegister.unsubscribe(id: id)
    }
    
    func subscribe(to topic: String, id: UUID) {
        self.subscriptionRegister.subscribe(topic: topic, id: id)
    }
    
    func unsubscribe(id: UUID) {
        self.subscriptionRegister.unsubscribe(id: id)
    }
    
    func broadcast<T: WebSocketMessage>(message: T, to topic: String? = nil) async throws {
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        let jsonData = try encoder.encode(message)
        
        guard let jsonString = String(data: jsonData, encoding: .utf8) else {
            throw BroadcastError.jsonEncodingFailed
        }
        
        let targetIds: Set<UUID>
        if let topic = topic {
            if let subscription = self.subscriptionRegister.findSubscription(topic: topic) {
                targetIds = subscription.ids
            } else {
                targetIds = Set()
            }
        } else {
            targetIds = Set(connections.keys)
        }
        
        for id in targetIds {
            if let writer = connections[id] {
                try await writer.write(.text(jsonString))
            }
        }
    }
}

