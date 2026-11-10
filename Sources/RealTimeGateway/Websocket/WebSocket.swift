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
    let resubscribePolicy: ResubscribePolicy
    var streamTasks = StreamTaskRegistry()

    public init(client: KurrentDBClient) {
        self.init(client: client, resubscribePolicy: .default)
    }

    public init(client: KurrentDBClient, resubscribePolicy: ResubscribePolicy = .default) {
        self.client = client
        self.connectionManager = ConnectionManager()
        self.resubscribePolicy = resubscribePolicy
    }


    public func subscribeStreams<S: StreamHelper>(streamHelpers: S...) async {
        await self.subscribeStreams(streamHelpers: streamHelpers)
    }

    public func subscribeStreams<S: StreamHelper>(streamHelpers: [S]) async {
        for streamHelper in streamHelpers {
            self.subscribe(streamName: streamHelper.name, processor: streamHelper.processor)
        }
    }

    /// 每條 stream 起一個 Task 跑重訂閱迴圈（永不放棄、指數退避）。
    /// 同一個 streamName 重複呼叫時，舊的迴圈會被取消並由新的取代（不會重複推送）。
    public func subscribe<P: EventMessageProcessor>(streamName: String, processor: P) {
        streamTasks.start(streamName: streamName) {
            Task {
                await runStreamSubscription(
                    streamName: streamName,
                    loop: ResubscribingLoop<ReadEvent>(policy: self.resubscribePolicy),
                    subscribe: { try await self.client.subscribeStream(streamName, configure: streamSubscriptionOptions).events },
                    process: { try processor.process(eventData: $0.record) },
                    resubscribedMessage: { processor.resubscribedMessage(streamName: streamName) },
                    broadcast: { try await self.connectionManager.broadcast(message: $0) }
                )
            }
        }
    }

    @available(*, deprecated, message: "retryCount／maxRetries 已無作用：改為永不放棄＋指數退避（ResubscribePolicy）。請改用 subscribe(streamName:processor:)")
    public func webSocketSubscribeKurrentDB<P: EventMessageProcessor>(streamName: String, eventProcessor: P, retryCount: Int = 0, maxRetries: Int = 10) {
        self.subscribe(streamName: streamName, processor: eventProcessor)
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
