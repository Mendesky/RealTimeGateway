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

    
    public func subscribeStreams<S: Stream>(streams: [S]) async {
        for stream in streams {
            self.webSocketSubscribeKurrentDB(streamName: stream.name, eventProcessor: stream.processor)
        }
        
    }
    
    public func webSocketSubscribeKurrentDB<P: EventMessageProcessor>(streamName: String, eventProcessor: P) {
        Task {
            
            let client = self.client
            do {
                let subscription = try await client.subscribeStream(streamName) {
                    $0.resolveLinks()
                }
                
                for try await result in subscription.events {
                    let message = try eventProcessor.start(eventData: result.record)
                    guard let message else {
                        logger.error("Error: The event does not have a corresponding message")
                        continue
                    }
                    
                    try await self.connectionManager.broadcast(message: message)
                }
            } catch {
                logger.error("The error happened when subscribing to persistent subscription: \(error)")
                
                try? await Task.sleep(nanoseconds: UInt64(1_000))
                
                self.webSocketSubscribeKurrentDB(streamName: streamName, eventProcessor: eventProcessor)
            }
        }
    }
        
    public func handleConnection(inbound: WebSocketInboundStream, outbound: WebSocketOutboundWriter) async {
        
        let id = UUID()
        await self.connectionManager.add(id: id, writer: outbound)
        
        do {
            for try await message in inbound {
                
            }
        } catch{
            logger.error("ConnectionError: \(error)")
        }
        await self.connectionManager.remove(id: id)
    }
    
}
