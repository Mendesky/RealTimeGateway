//
//  WebSocketMessage.swift
//  RealTimeGateway
//
//  Created by 蔡東棋 on 2025/12/3.
//

public typealias StandardWebSocketMessage = GatewayMessage<StandardPayload>

public protocol WebSocketPayload: Codable, Sendable {}

public protocol WebSocketMessage: Codable, Sendable {
    associatedtype Payload: WebSocketPayload
    var type: String { get }
    var payload: Payload { get }
}

public struct GatewayMessage<P: WebSocketPayload>: WebSocketMessage {
    
    public typealias Payload = P
    public var type: String
    public var payload: P
    
    public init(type: String, payload: P) {
        self.type = type
        self.payload = payload
    }
}

    
public struct StandardPayload: WebSocketPayload {
    var quotationId: String?
    var quotingCaseId: String?
    var quotingBundleId: String?
    var quotingUnitId: String?
    var addtionalQuotingUnitId: String?
    var serviceScopeId: String?
    var businessClientId: String?
    var customMetadata: CustomMetadata
    
    public init(
        quotationId: String? = nil,
        quotingCaseId: String? = nil,
        quotingBundleId: String? = nil,
        quotingUnitId: String? = nil,
        addtionalQuotingUnitId: String? = nil,
        serviceScopeId: String? = nil,
        businessClientId: String? = nil,
        customMetadata: CustomMetadata
    ) {
        self.quotationId = quotationId
        self.quotingCaseId = quotingCaseId
        self.quotingBundleId = quotingBundleId
        self.quotingUnitId = quotingUnitId
        self.addtionalQuotingUnitId = addtionalQuotingUnitId
        self.serviceScopeId = serviceScopeId
        self.businessClientId = businessClientId
        self.customMetadata = customMetadata
    }
}

public struct CustomMetadata: Codable, Sendable {
    var userId: String?
}
