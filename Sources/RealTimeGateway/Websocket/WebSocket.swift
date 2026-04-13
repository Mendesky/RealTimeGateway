//
//  WebSocket.swift
//  RealTimeGateway
//
//  Created by 蔡東棋 on 2025/11/27.
//
import KurrentDB
import KurrentDB_V1
import Logging
import Foundation
import Hummingbird
import HummingbirdWebSocket

let logger = Logger(label: "WebSocketServer")

public protocol StreamHelper {
    associatedtype T: EventMessageProcessor
    var name: String { get }
    var processor: T { get }
}

public struct StandardStream<T: EventMessageProcessor>: StreamHelper {
    public var name: String
    
    public var processor: T
    
    public init(name: String, processor: T) {
        self.name = name
        self.processor = processor
    }
}

public typealias StandardWebSocket = WebSocket<StandardWebSocketMessage>

public actor WebSocket<W: WebSocketMessage> {
    
    var client: KurrentDBClient
    var connectionManager: ConnectionManager
    
    public init(client: KurrentDBClient) {
        self.client = client
        self.connectionManager = ConnectionManager()
    }

    
    public func subscribeStreams<S: StreamHelper>(streamHelpers: S...) async {
        await self.subscribeStreams(streamHelpers: streamHelpers)
    }
    
    public func subscribeStreams<S: StreamHelper>(streamHelpers: [S]) async {
        for streamHelper in streamHelpers {
            self.webSocketSubscribeKurrentDB(streamName: streamHelper.name, eventProcessor: streamHelper.processor)
        }
    }
    
    public func webSocketSubscribeKurrentDB<P: EventMessageProcessor>(streamName: String, eventProcessor: P, retryCount: Int = 0, maxRetries: Int = 10) {
        Task {
            
            let client = self.client
            do {
                let subscription = try await client.subscribeStream(streamName) {
                    $0.resolveLinks()
                }
                
                for try await result in subscription.events {
                    let message = try eventProcessor.process(eventData: result.record)
                    guard let message else {
                        logger.error("Error: The event does not have a corresponding message")
                        continue
                    }
                    
                    try await self.connectionManager.broadcast(message: message)
                }
            } catch {
                logger.error("The error happened when subscribing to persistent subscription: \(error)")
                guard retryCount < maxRetries else {
                    logger.error("Max retries reached for stream: \(streamName)")
                    return
                }
                try? await Task.sleep(nanoseconds: UInt64(100_000_000))
                
                self.webSocketSubscribeKurrentDB(streamName: streamName, eventProcessor: eventProcessor, retryCount: retryCount + 1, maxRetries: maxRetries)
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
