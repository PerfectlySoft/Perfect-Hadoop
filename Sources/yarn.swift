//
//  yarn.swift
//  PerfectHadoop
//
//  Created by Rockford Wei on 2016-11-04.
//	Copyright (C) 2015 PerfectlySoft, Inc.
//
//===----------------------------------------------------------------------===//
//
// This source file is part of the Perfect.org open source project
//
// Copyright (c) 2016 - 2017 PerfectlySoft Inc. and the Perfect project authors
// Licensed under Apache License v2.0
//
// See http://perfect.org/licensing.html for license information
//
//===----------------------------------------------------------------------===//
//

import cURL
import PerfectCURL
import PerfectLib
import Foundation

public class WebYarn: WebHDFS {

  /// constructor of WebYarn
  /// - parameters:
  ///   - service: the protocol of web request - http / https / webhdfs / hdfs
  ///		- host: the hostname or ip address of the webhdfs host
  ///		- port: the port of webhdfs host
  ///		- auth: Authorization Model. Please check the enum Authentication for details
  ///   - proxyUser: proxy user, if applicable
  ///   - extraHeaders: extra headers if special header such as CSRF (Cross-Site Request Forgery Prevention) is applicable
  ///		- apibase: use this parameter ONLY the target server has a different api routine other than /webhdfs/v1
  ///   - timeout: timeout in seconds, zero means never timeout during transfer
  public override init (service: String = "http",
               host: String = "localhost", port: Int = 8088,
               auth: Authentication = .off, proxyUser: String = "",
               extraHeaders: [String] = [],
               apibase: String = "/ws/v1/cluster", timeout: Int = 0) {
    super.init(service: service, host: host, port: port, auth: auth, proxyUser: proxyUser, extraHeaders: extraHeaders, apibase: apibase, timeout: timeout)
  }//end constructor


  public class ClusterInfo : JSONStruct {
    var id: Int = 0
    var startedOn: Int = 0
    var state: String = ""
    var haState: String = ""
    var rmStateStoreName: String = ""
    var resourceManagerVersion : String = ""
    var resourceManagerBuildVersion: String = ""
    var resourceManagerVersionBuiltOn: String = ""
    var hadoopVersion: String = ""
    var hadoopBuildVersion: String = ""
    var hadoopVersionBuiltOn: String = ""
    var haZooKeeperConnectionState: String = ""
  }//end ClusterInfo

  public func clusterInfo() throws -> ClusterInfo {
    let url = "\(service)://\(host):\(port)\(base)/"
    let (header, body, res, _) = try perform(overwriteURL: url)
    guard let info = res["clusterInfo"] as? [String:Any] else {
      throw Exception.unexpectedResponse(url: url, header: header, body: body)
    }//end guard
    return try ClusterInfo(dictionary: info)
  }//end clusterInfo

  public class Metrics : JSONStruct {
    var appsSubmitted: Int = 0
    var appsCompleted: Int = 0
    var appsPending: Int = 0
    var appsRunning: Int = 0
    var appsFailed: Int = 0
    var appsKilled: Int = 0
    var reservedMB: Int = 0
    var availableMB: Int = 0
    var allocatedMB: Int = 0
    var reservedVirtualCores: Int = 0
    var availableVirtualCores: Int = 0
    var allocatedVirtualCores: Int = 0
    var containersAllocated:Int = 0
    var containersReserved:Int = 0
    var containersPending:Int = 0
    var totalMB: Int = 0
    var totalVirtualCores: Int = 0
    var totalNodes: Int = 0
    var lostNodes:Int = 0
    var unhealthyNodes: Int = 0
    var decommissioningNodes:Int = 0
    var decommissionedNodes:Int = 0
    var rebootedNodes:Int = 0
    var activeNodes: Int = 0
    var shutdownNodes: Int = 0
  }//end Metrics

  public func metrics() throws -> Metrics {
    let url = "\(service)://\(host):\(port)\(base)/metrics"
    let (header, body, res, _) = try perform(overwriteURL: url)
    guard let info = res["clusterMetrics"] as? [String:Any] else {
      throw Exception.unexpectedResponse(url: url, header: header, body: body)
    }//end guard
    return try Metrics(dictionary: info)
  }//end metrics

  public func scheduler() throws -> [String:Any] {
    let url = "\(service)://\(host):\(port)\(base)/scheduler"
    let (header, body, res, _) = try perform(overwriteURL: url)
    guard let sch = res["scheduler"] as? [String:Any] else {
      throw Exception.unexpectedResponse(url: url, header: header, body: body)
    }//end guard
    guard let info = sch["schedulerInfo"] as? [String:Any] else {
      throw Exception.unexpectedResponse(url: url, header: header, body: body)
    }//end guard
    return info
  }//end scheduler

  public class App : JSONStruct {
    var id: String = ""
    var user: String = ""
    var name: String = ""
    var queue: String = ""
    var state: String = ""
    var finalStatus: String = ""
    var progress: Int = 0
    var trackingUI: String = ""
    var trackingUrl: String = ""
    var diagnostics: String = ""
    var clusterId: ULONG = 0
    var applicationType : String = ""
    var applicationTags: String = ""
    var priority: Int = 0
    var startedTime: ULONG = 0
    var finishedTime: ULONG = 0
    var elapsedTime: ULONG = 0
    var amContainerLogs: String = ""
    var amHostHttpAddress: String = ""
    var amRPCAddress: String = ""
    var allocatedMB: Int = 0
    var allocatedVCores: Int = 0
    var runningContainers: Int = 0
    var memorySeconds: Int = 0
    var vcoreSeconds: Int = 0
    var queueUsagePercentage: Int = 0
    var clusterUsagePercentage: Int = 0
    var preemptedResourceMB: Int = 0
    var preemptedResourceVCores: Int = 0
    var numNonAMContainerPreempted: Int = 0
    var numAMContainerPreempted: Int = 0
    var logAggregationStatus: String = ""
    var unmanagedApplication: Bool = false
    var amNodeLabelExpression: String = ""
  }//end App

  /// - parameters:
  ///   - states - applications matching the given application states, specified as a comma-separated list.
  ///   - finalStatus - the final status of the application - reported by the application itself
  ///   - user - user name
  ///   - queue - queue name
  ///   - limit - total number of app objects to be returned
  ///   - startedTimeBegin - applications with start time beginning with this time, specified in ms since epoch
  ///   - startedTimeEnd - applications with start time ending with this time, specified in ms since epoch
  ///   - finishedTimeBegin - applications with finish time beginning with this time, specified in ms since epoch
  ///   - finishedTimeEnd - applications with finish time ending with this time, specified in ms since epoch
  ///   - applicationTypes - applications matching the given application types, specified as a comma-separated list.
  ///   - applicationTags - applications matching any of the given application tags, specified as a comma-separated list.
  public func apps(
      states:[String]=[],
      finalStatus: String = "",
      user: String = "",
      queue: String = "",
      limit: Int = 0,
      startedTimeBegin: Int = -1,
      startedTimeEnd: Int = -1,
      finishedTimeBegin: Int = -1,
      finishedTimeEnd: Int = -1,
      applicationTypes: [String] = [],
      applicationTags: [String] = []
    ) throws -> [App] {

    var p: [String: Any] = [:]

    if states.count > 0 {
      p["states"] = states.reduce("") { $0 + "," + $1}
    }//end if

    if !finalStatus.isEmpty {
      p["finalStatus"] = finalStatus
    }//end if

    if !user.isEmpty {
      p["user"] = user
    }//end if

    if !queue.isEmpty {
      p["queue"] = queue
    }//end if

    if limit > 0 {
      p["limit"] = limit
    }//end if

    if startedTimeBegin > 0 {
      p["startedTimeBegin"] = startedTimeBegin
    }//end if

    if startedTimeEnd > 0 {
      p["startedTimeEnd"] = startedTimeEnd
    }//end if

    if finishedTimeBegin > 0 {
      p["finishedTimeBegin"] = finishedTimeBegin
    }//end if

    if finishedTimeEnd > 0 {
      p["finishedTimeEnd"] = finishedTimeEnd
    }//end if

    if applicationTypes.count > 0 {
      p["applicationTypes"] = applicationTypes.reduce("") { $0 + "," + $1}
    }//end if

    if applicationTags.count > 0 {
      p["applicationTags"] = applicationTags.reduce("") { $0 + "," + $1}
    }//end if


    var url = "\(service)://\(host):\(port)\(base)/apps"

    if p.count > 0 {
      let par = p.reduce("") { $0 + "&\($1.key)=\($1.value)"}
      url.append("&\(par)")
    }//end if

    let (header, body, res, _) = try perform(overwriteURL: url)
    guard let raw = res["apps"] as? [String:Any] else {
      throw Exception.unexpectedResponse(url: url, header: header, body: body)
    }//end guard
    guard let info = raw["app"] as? [[String:Any]] else {
      throw Exception.unexpectedResponse(url: url, header: header, body: body)
    }//end guard
    return info.map{ try! App(dictionary: $0) }
  }//end apps

  public class AppStatItem: JSONStruct {
    var state: String = ""
    var type: String = ""
    var count: Int = 0
  }//end class

  public func appStatInfo(states:[String] = [], applicationTypes:[String] = []) throws -> [AppStatItem] {

    var p: [String: Any] = [:]

    if states.count > 0 {
      p["states"] = states.reduce("") { $0 + "," + $1}
    }//end if

    if applicationTypes.count > 0 {
      p["applicationTypes"] = applicationTypes.reduce("") { $0 + "," + $1}
    }//end if

    var url = "\(service)://\(host):\(port)\(base)/appstatistics"

    if p.count > 0 {
      let par = p.reduce("") { $0 + "&\($1.key)=\($1.value)"}
      url.append("&\(par)")
    }//end if

    let (header, body, res, _) = try perform(overwriteURL: url)
    guard let raw = res["appStatInfo"] as? [String:Any] else {
      throw Exception.unexpectedResponse(url: url, header: header, body: body)
    }//end guard
    guard let items = raw["statItem"] as? [[String:Any]] else {
      throw Exception.unexpectedResponse(url: url, header: header, body: body)
    }//end guard
    return items.map{ try! AppStatItem(dictionary: $0) }
  }//end func

  public func getApp(id:String) throws -> App {
    let url = "\(service)://\(host):\(port)\(base)/apps/\(id)"
    let (header, body, res, _) = try perform(overwriteURL: url)
    guard let raw = res["app"] as? [String:Any] else {
      throw Exception.unexpectedResponse(url: url, header: header, body: body)
    }//end guard
    return try App(dictionary: raw)
  }//end getApp

  public class AppAttempt : JSONStruct {
    var id: Int = 0
    var nodeId: String = ""
    var nodeHttpAddress: String = ""
    var nodesBlacklistedBySystem: String = ""
    var logsLink: String = ""
    var containerId: String = ""
    var startTime: Int = 0
    var finishedTime: Int = 0
    var appAttemptId: String = ""
    var blacklistedNodes: String = ""
  }//end AppAttempt

  public func getAttempts(id:String) throws -> [AppAttempt] {
    let url = "\(service)://\(host):\(port)\(base)/apps/\(id)/appattempts"
    let (header, body, res, _) = try perform(overwriteURL: url)
    guard let raw = res["appAttempts"] as? [String:Any] else {
      throw Exception.unexpectedResponse(url: url, header: header, body: body)
    }//end guard
    guard let items = raw["appAttempt"] as? [[String:Any]] else {
      throw Exception.unexpectedResponse(url: url, header: header, body: body)
    }//end guard
    return items.map{ try! AppAttempt(dictionary: $0) }
  }//end getAttempts

  public class Node : JSONStruct {
    var rack: String = ""
    var state: String = ""
    var id: String = ""
    var nodeHostName: String = ""
    var nodeHTTPAddress: String = ""
    var healthStatus: String = ""
    var lastHealthUpdate: Int = 0
    var healthReport: String = ""
    var numContainers: Int = 0
    var usedMemoryMB: Int = 0
    var availMemoryMB: Int = 0
    var usedVirtualCores: Int = 0
    var availableVirtualCores: Int = 0
    var resourceUtilization: [String:Any] = [:]
    var version: String = ""
  }//end class Node

  public func getNodes() throws -> [Node] {
    let url = "\(service)://\(host):\(port)\(base)/nodes"
    let (header, body, res, _) = try perform(overwriteURL: url)
    guard let raw = res["nodes"] as? [String:Any] else {
      throw Exception.unexpectedResponse(url: url, header: header, body: body)
    }//end guard
    guard let items = raw["node"] as? [[String:Any]] else {
      throw Exception.unexpectedResponse(url: url, header: header, body: body)
    }//end guard
    return items.map{ try! Node(dictionary: $0) }
  }//end getNodes

  public func getNode(id: String) throws -> Node {
    let url = "\(service)://\(host):\(port)\(base)/nodes/\(id)"
    let (header, body, res, _) = try perform(overwriteURL: url)
    guard let node = res["node"] as? [String:Any] else {
      throw Exception.unexpectedResponse(url: url, header: header, body: body)
    }//end guard
    return try Node(dictionary: node)
  }//end getNode
}//end webyarn
