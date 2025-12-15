//
//  ConnectionManager.swift
//  RealTimeGateway
//
//  Created by 蔡東棋 on 2025/11/27.
//

import HummingbirdWebSocket
import Foundation


    
package actor ConnectionManager {
    
    var connections: [UUID: WebSocketOutboundWriter] = [:]
    var subscriptions: [String: Set<UUID>] = [:]
    
    func add(id: UUID, writer: WebSocketOutboundWriter) {
        connections[id] = writer
    }
    
    func remove(id: UUID) {
        connections.removeValue(forKey: id)
        // Remove from all subscriptions
        for (topic, subscribers) in subscriptions {
            var newSubscribers = subscribers
            newSubscribers.remove(id)
            if newSubscribers.isEmpty {
                subscriptions.removeValue(forKey: topic)
            } else {
                subscriptions[topic] = newSubscribers
            }
        }
    }
    
    func subscribe(to topic: String, id: UUID) {
        var subscribers = subscriptions[topic] ?? []
        subscribers.insert(id)
        subscriptions[topic] = subscribers
    }
    
    func unsubscribe(from topic: String, id: UUID) {
        var subscribers = subscriptions[topic] ?? []
        subscribers.remove(id)
        if subscribers.isEmpty {
            subscriptions.removeValue(forKey: topic)
        } else {
            subscriptions[topic] = subscribers
        }
    }
    
    func broadcast(message: WebSocketMessage, to topic: String? = nil) async throws {
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        let jsonData = try encoder.encode(message)
        
        guard let jsonString = String(data: jsonData, encoding: .utf8) else { return }
        
        let targetIds: Set<UUID>
        if let topic = topic {
            targetIds = subscriptions[topic] ?? []
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

