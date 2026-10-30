//
//  EventConvertor.swift
//  RealTimeGateway
//
//  Created by 蔡東棋 on 2025/11/27.
//
import Foundation
import KurrentDB
import KurrentDB_V1


public struct StandardEventMessageProcessor: EventMessageProcessor {
    
    public init() {}
    
    public typealias MessageType = StandardWebSocketMessage
    
    public func process(eventData: RecordedEvent) throws -> MessageType? {
        let payload = try eventData.decode(to: MessageType.Payload.self)
        guard let payload else { return nil }
        return .init(type: eventData.eventType, payload: payload) //可放 metadata
    }
}

public protocol EventMessageProcessor {
    associatedtype MessageType: WebSocketMessage
    func process(eventData: RecordedEvent) throws -> MessageType?
    /// 非第一次成功訂閱後呼叫；回傳非 nil 就廣播給所有連線。預設 nil（行為與 1.1.0 相同）。
    func resubscribedMessage(streamName: String) -> MessageType?
}

extension EventMessageProcessor {
    public func resubscribedMessage(streamName: String) -> MessageType? { nil }
}
