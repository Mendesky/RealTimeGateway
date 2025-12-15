//
//  WebSocket.swift
//  RealTimeGateway
//
//  Created by 蔡東棋 on 2025/11/27.
//
import KurrentDB
import Logging
import Foundation
import Hummingbird
import HummingbirdWebSocket

let logger = Logger(label: "WebSocketServer")


public protocol Stream {
    associatedtype T: EventMessageProcessor
    var name: String { get }
    var processor: T { get }
}

public struct StandardStream<T: EventMessageProcessor>: Stream {
    public var name: String
    
    public var processor: T
    
    public typealias T = T
    
    public init(name: String, processor: T) {
        self.name = name
        self.processor = processor
    }
}

public typealias standardWebSocket = WebSocket<StandardWebSocketMessage>

public actor WebSocket<W: WebSocketMessage> {
    
    var client: KurrentDBClient
    var connectionManager: ConnectionManager
    
    public init(client: KurrentDBClient) {
        self.client = client
        self.connectionManager = ConnectionManager()
    }

    
    public func subscribeStreams(streams: [Stream], groupName: String) async {
        for stream in streams {
            do {
                try await self.client.createPersistentSubscription(stream: stream.name, groupName: groupName) {
                    $0.resolveLink()
                }
            } catch let error as KurrentError {
                logger.error("create persistent subscription error: \(error)")
            } catch {
                logger.error("handle subscription error:\(error)")
            }

            self.webSocketSubscribeKurrentDB(streamName: stream.name, groupName: groupName, eventProcessor: stream.processor)
        }
        
    }
    
    public func webSocketSubscribeKurrentDB(streamName: String, groupName: String, eventProcessor: EventMessageProcessor) {
        Task {
            // If `client` is Sendable (or thread-safe), capture it locally
            let client = self.client
            do {
                let subscription = try await client.subscribePersistentSubscription(
                    stream: streamName,
                    groupName: groupName
                )
                
                
                for try await result in subscription.events {
                    let message = try eventProcessor.start(eventData: result.event.record)
                    guard let message else {
                        logger.error("Error: The event does not have a corresponding message")
                        continue
                    }
                    
                    try await self.connectionManager.broadcast(message: message)
                    try await subscription.ack(readEvents: result.event)
                }
            } catch {
                logger.error("The error happened when subscribing to persistent subscription: \(error)")
                
                try? await Task.sleep(nanoseconds: UInt64(1_000))
                
                self.webSocketSubscribeKurrentDB(streamName: streamName, groupName: groupName, eventProcessor: eventProcessor)
            }
        }
    }
    
    public func subscribeAll() {
        
    }
    
    public func handleConnection(inbound: WebSocketInboundStream, outbound: WebSocketOutboundWriter) async {
        
        let id = UUID()
        await self.connectionManager.add(id: id, writer: outbound)
        
        do {
            for try await _ in inbound {
                
            }
        } catch{
            logger.error("ConnectionError: \(error)")
        }
        await self.connectionManager.remove(id: id)
    }
    
}
