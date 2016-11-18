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
    print(url)
    let (header, body, res, _) = try perform(overwriteURL: url)
    guard let info = res["clusterInfo"] as? [String:Any] else {
      throw Exception.unexpectedResponse(url: url, header: header, body: body)
    }//end guard
    let inf = try ClusterInfo(dictionary: info)
    return inf
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
    print(url)
    let (header, body, res, _) = try perform(overwriteURL: url)
    guard let info = res["clusterMetrics"] as? [String:Any] else {
      throw Exception.unexpectedResponse(url: url, header: header, body: body)
    }//end guard
    let m = try Metrics(dictionary: info)
    return m
  }//end metrics
}
