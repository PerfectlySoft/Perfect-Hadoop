//
//  YARNRMData.swift
//  PerfectHadoop - YARN Resource Manager Data Structures
//
//  Created by Rockford Wei on 2016-01-05.
//	Copyright (C) 2017 PerfectlySoft, Inc.
//
//===----------------------------------------------------------------------===//
//
// This source file is part of the Perfect.org open source project
//
// Copyright (c) 2017 - 2018 PerfectlySoft Inc. and the Perfect project authors
// Licensed under Apache License v2.0
//
// See http://perfect.org/licensing.html for license information
//
//===----------------------------------------------------------------------===//
//

import PerfectLib



public struct FairQueue {
  var maxApps = 0
  var minResources = ResourcesUsed([:])
  var maxResources = ResourcesUsed([:])
  var usedResources = ResourcesUsed([:])
  var fairResources = ResourcesUsed([:])
  var clusterResources = ResourcesUsed([:])
  var queueName = ""
  var schedulingPolicy = ""
  var childQueues = [FairQueue]()
  var type = ""
  var numActiveApps = 0
  var numPendingApps = 0
  public init(_ dictionary: [String:Any] = [:]) {
    self.maxApps = dictionary["maxApps"] as? Int ?? 0
    self.minResources = ResourcesUsed(dictionary["minResources"] as? [String:Any] ?? [:])
    self.maxResources = ResourcesUsed(dictionary["maxResources"] as? [String:Any] ?? [:])
    self.usedResources = ResourcesUsed(dictionary["usedResources"] as? [String:Any] ?? [:])
    self.fairResources = ResourcesUsed(dictionary["fairResources"] as? [String:Any] ?? [:])
    self.clusterResources = ResourcesUsed(dictionary["clusterResources"] as? [String:Any] ?? [:])
    self.queueName = dictionary["queueName"] as? String ?? ""
    self.type = dictionary["type"] as? String ?? ""
    self.numActiveApps = dictionary["numActiveApps"] as? Int ?? 0
    self.numPendingApps = dictionary["numPendingApps"] as? Int ?? 0
    let q = dictionary["childQueues"] as? [String:Any] ?? [:]
    self.childQueues = (q["queue"] as? [Any] ?? []).map {FairQueue($0 as? [String:Any] ?? [:])}
  }//init
}// FairQueue


public struct APP {
  var allocatedMB = 0
  var allocatedVCores = 0
  var amContainerLogs = ""
  var amHostHttpAddress = ""
  var amRPCAddress = ""
  var amNodeLabelExpression = ""
  var applicationPriority = 0
  var applicationTags = ""
  var applicationType = ""
  var appNodeLabelExpression = ""
  var containerids = [String]()
  var clusterId = 0
  var diagnostics = ""
  var elapsedTime = 0
  var finalStatus = ""
  var finishedTime = 0
  var id = ""
  var memorySeconds = 0
  var name = ""
  var progress: Double = 0.0
  var queue = ""
  var runningContainers = 0
  var startedTime = 0
  var state = ""
  var trackingUI = ""
  var trackingUrl = ""
  var unmanagedApplication = ""
  var user = ""
  var vcoreSeconds = 0
  public init(_ dictionary: [String:Any] = [:]) {
    self.allocatedMB = dictionary["allocatedMB"] as? Int ?? 0
    self.allocatedVCores = dictionary["allocatedVCores"] as? Int ?? 0
    self.amContainerLogs = dictionary["amContainerLogs"] as? String ?? ""
    self.amHostHttpAddress = dictionary["amHostHttpAddress"] as? String ?? ""
    self.amRPCAddress = dictionary["amRPCAddress"] as? String ?? ""
    self.amNodeLabelExpression = dictionary["amnodeLabelExpression"] as? String ?? ""
    self.applicationPriority = dictionary["applicationPriority"] as? Int ?? 0
    self.applicationTags = dictionary["applicationTags"] as? String ?? ""
    self.applicationType = dictionary["applicationType"] as? String ?? ""
    self.appNodeLabelExpression = dictionary["appNodeLabelExpression"] as? String ?? ""
    self.containerids = dictionary["containerids"] as? [String] ?? []
    self.clusterId = dictionary["clusterId"] as? Int ?? 0
    self.diagnostics = dictionary["diagnostics"] as? String ?? ""
    self.elapsedTime = dictionary["elapsedTime"] as? Int ?? 0
    self.finalStatus = dictionary["finalStatus"] as? String ?? ""
    self.finishedTime = dictionary["finishedTime"] as? Int ?? 0
    self.id = dictionary["id"] as? String ?? ""
    self.memorySeconds = dictionary["memorySeconds"] as? Int ?? 0
    self.name = dictionary["name"] as? String ?? ""
    self.progress = Double(any: dictionary["progress"])
    self.queue = dictionary["queue"] as? String ?? ""
    self.runningContainers = dictionary["runningContainers"] as? Int ?? 0
    self.startedTime = dictionary["startedTime"] as? Int ?? 0
    self.state = dictionary["state"] as? String ?? ""
    self.trackingUI = dictionary["trackingUI"] as? String ?? ""
    self.trackingUrl = dictionary["trackingUrl"] as? String ?? ""
    self.unmanagedApplication = dictionary["unmanagedApplication"] as? String ?? ""
    self.user = dictionary["user"] as? String ?? ""
    self.vcoreSeconds = dictionary["vcoreSeconds"] as? Int ?? 0
  }//init
}//APP

extension String {
  public var asApp: APP? {
    get {
      do {
        let dic = try self.jsonDecode() as? [String:Any] ?? [:]
        return APP(dic["app"] as? [String: Any] ?? [:])
      }catch {
        return nil
      }//end do
    }//end get
  }//end member
}//end extension

extension String {
  public var asApps: [APP] {
    get {
      do {
        let dic = try self.jsonDecode() as? [String:Any] ?? [:]
        let apps = dic["apps"] as? [String:Any] ?? [:]
        return (apps["app"] as? [Any] ?? []).map { APP($0 as? [String: Any] ?? [:])}
      }catch {
        return []
      }//end do
    }//end get
  }//end member
}//end extension

public struct AppStatInfo{
  public struct StatItem{
    var count = 0
    var state = ""
    var type = ""
    public init(_ dictionary: [String:Any] = [:]) {
      self.count = dictionary["count"] as? Int ?? 0
      self.state = dictionary["state"] as? String ?? ""
      self.type = dictionary["type"] as? String ?? ""
    }//init
  }//statItemItem
  var statItem : [StatItem] = []
  public init(_ dictionary: [String:Any] = [:]) {
    self.statItem = (dictionary["statItem"] as? [Any] ?? []).map{StatItem($0 as? [String : Any] ?? [:])}
  }//init
}//Appstatinfo

extension String {
  public var asAppStatInfo: AppStatInfo? {
    get{
      do{
        let dic = try self.jsonDecode() as? [String:Any] ?? [:]
        return AppStatInfo(dic["appStatInfo"] as? [String:Any] ?? [:])
      }catch{
        return nil
      }//end do
    }//end get
  }//end member
}//end extension

public struct AppAttempt{
  var containerId = ""
  var id = 0
  var logsLink = ""
  var nodeHttpAddress = ""
  var nodeId = ""
  var startTime = 0
  public init(_ dictionary: [String:Any] = [:]) {
    self.containerId = dictionary["containerId"] as? String ?? ""
    self.id = dictionary["id"] as? Int ?? 0
    self.logsLink = dictionary["logsLink"] as? String ?? ""
    self.nodeHttpAddress = dictionary["nodeHttpAddress"] as? String ?? ""
    self.nodeId = dictionary["nodeId"] as? String ?? ""
    self.startTime = dictionary["startTime"] as? Int ?? 0
  }//init
}//Appattempt

extension String {
  public var asAppAttempts: [AppAttempt] {
    get{
      do{
        let dic = try self.jsonDecode() as? [String:Any] ?? [:]
        let a = dic["appAttempts"] as? [String:Any] ?? [:]
        return (a["appAttempt"] as? [Any] ?? []).map{ AppAttempt( $0 as? [String:Any] ?? [:]) }
      }catch{
        return []
      }//end do
    }//end get
  }//end member
}//end extension

public struct Node{
  var availMemoryMB = 0
  var availableVirtualCores = 0
  var healthReport = ""
  var healthStatus = ""
  var id = ""
  var lastHealthUpdate = 0
  var nodeHTTPAddress = ""
  var nodeHostName = ""
  var numContainers = 0
  var rack = ""
  var state = ""
  var usedMemoryMB = 0
  var usedVirtualCores = 0
  public init(_ dictionary: [String:Any] = [:]) {
    self.availMemoryMB = dictionary["availMemoryMB"] as? Int ?? 0
    self.availableVirtualCores = dictionary["availableVirtualCores"] as? Int ?? 0
    self.healthReport = dictionary["healthReport"] as? String ?? ""
    self.healthStatus = dictionary["healthStatus"] as? String ?? ""
    self.id = dictionary["id"] as? String ?? ""
    self.lastHealthUpdate = dictionary["lastHealthUpdate"] as? Int ?? 0
    self.nodeHTTPAddress = dictionary["nodeHTTPAddress"] as? String ?? ""
    self.nodeHostName = dictionary["nodeHostName"] as? String ?? ""
    self.numContainers = dictionary["numContainers"] as? Int ?? 0
    self.rack = dictionary["rack"] as? String ?? ""
    self.state = dictionary["state"] as? String ?? ""
    self.usedMemoryMB = dictionary["usedMemoryMB"] as? Int ?? 0
    self.usedVirtualCores = dictionary["usedVirtualCores"] as? Int ?? 0
  }//init
}//Node

extension String {
  public var asNode: Node? {
    get{
      do{
        let dic = try self.jsonDecode() as? [String:Any] ?? [:]
        return Node(dic["node"] as? [String:Any] ?? [:])
      }catch{
        return nil
      }//end do
    }//end get
  }//end member
  public var asNodes: [Node] {
    get{
      do{
        let dic = try self.jsonDecode() as? [String:Any] ?? [:]
        let n = dic["nodes"] as? [String:Any] ?? [:]
        return (n["node"] as? [Any] ?? []).map { Node($0 as? [String:Any] ?? [:]) }
      }catch{
        return []
      }//end do
    }//end get
  }//end member
}//end extension

public struct NewApplication {
  var id = ""
  var maximumResourceCapability = ResourcesUsed()
  public init(_ dictionary: [String:Any] = [:]) {
    self.id = dictionary["application-id"] as? String ?? ""
    self.maximumResourceCapability = ResourcesUsed(dictionary["maximum-resource-capability"] as? [String:Any] ?? [:])
  }//end init
}//end structure

extension String {
  public var asNewApplication: NewApplication? {
    get{
      do{
        return NewApplication(try self.jsonDecode() as? [String:Any] ?? [:])
      }catch{
        return nil
      }//end do
    }//end get
  }
}//end extension

public struct Token {
  var expirationTime = 0
  var kind = ""
  var maxValidity = 0
  var owner = ""
  var renewer = ""
  var token = ""
  public init(_ dictionary: [String:Any] = [:]) {
    self.expirationTime = dictionary["expiration-time"] as? Int ?? 0
    self.kind = dictionary["kind"] as? String ?? ""
    self.maxValidity = dictionary["max-validity"] as? Int ?? 0
    self.owner = dictionary["owner"] as? String ?? ""
    self.renewer = dictionary["renewer"] as? String ?? ""
    self.token = dictionary["token"] as? String ?? ""
  }//init
}//Token

extension String {
  public var asToken: Token? {
    get{
      do{
        return Token(try self.jsonDecode() as? [String:Any] ?? [:])
      }catch{
        return nil
      }//end do
    }//end get
  }//end member
}//end extension



public struct Reservations{

  public struct Resource{
    var memory = ""
    var vCores = ""
    public init(_ dictionary: [String:Any] = [:]) {
      self.memory = dictionary["memory"] as? String ?? ""
      self.vCores = dictionary["vCores"] as? String ?? ""
    }//init
  }//resourcesUsedType

  public struct ResourceAllocation {
    var resource = Resource()
    var startTime = ""
    var endTime = ""
    public init(_ dictionary: [String:Any] = [:]) {
      self.resource = Resource(dictionary["resource"] as? [String:Any] ?? [:] )
      self.startTime = dictionary["startTime"] as? String ?? ""
      self.endTime = dictionary["endTime"] as? String ?? ""
    }//end init
  }//end struct

  public struct ReservationDefinition{

    public struct RerservationRequests {
      public struct ReservationRequest{
        var capability = Resource()
        var duration = ""
        var minConcurrency = ""
        var numContainers = ""
        public init(_ dictionary: [String:Any] = [:]) {
          self.capability = Resource(dictionary["capability"] as? [String:Any] ?? [:])
          self.duration = dictionary["duration"] as? String ?? ""
          self.minConcurrency = dictionary["min-concurrency"] as? String ?? ""
          self.numContainers = dictionary["num-containers"] as? String ?? ""
        }//init
      }//reservation-requestType

      var reservationRequestInterpreter = ""
      var reservationRequest = ReservationRequest()
      public init(_ dictionary: [String:Any] = [:]) {
        self.reservationRequestInterpreter = dictionary["reservation-request-interpreter"] as? String ?? ""
        self.reservationRequest = ReservationRequest(dictionary["reservation-request"] as? [String:Any] ?? [:])
      }//init
    }//end struct

    var arrival = ""
    var deadline = ""
    var reservationName = ""
    var reservationRequests = RerservationRequests()
    public init(_ dictionary: [String:Any] = [:]) {
      self.arrival = dictionary["arrival"] as? String ?? ""
      self.deadline = dictionary["deadline"] as? String ?? ""
      self.reservationName = dictionary["reservation-name"] as? String ?? ""
      self.reservationRequests = RerservationRequests(dictionary["reservation-requests"] as? [String:Any] ?? [:])
    }//init
  }//end struct

  var acceptanceTime = ""
  var user = ""
  var resourceAllocations = [ResourceAllocation]()
  var reservationId = ""
  var reservationDefinition = ReservationDefinition()
  public init (_ dictionary: [String:Any] = [:]) {
    self.acceptanceTime = dictionary["acceptance-time"] as? String ?? ""
    self.user = dictionary["user"] as? String ?? ""
    self.resourceAllocations = (dictionary["resource-allocations"] as? [Any] ?? []).map {ResourceAllocation($0 as? [String:Any] ?? [:]) }
    self.reservationId = dictionary["reservation-id"] as? String ?? ""
    self.reservationDefinition = ReservationDefinition(dictionary["reservation-definition"] as? [String:Any] ?? [:])
  }//end init
}//end struct

extension String {
  public var asReservations: Reservations? {
    get{
      do{
        let dic = try self.jsonDecode() as? [String:Any] ?? [:]
        return Reservations(dic["reservations"] as? [String:Any] ?? [:])
      }catch{
        return nil
      }//end do
    }//end get
  }//end member
}//end extension

