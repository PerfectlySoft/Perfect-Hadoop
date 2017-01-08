//
//  YARNResourceManager.swift
//  PerfectHadoop
//
//  Created by Rocky Wei on 2017-01-07.
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

/// The cluster information resource provides overall information about the cluster.
public struct ClusterInfo{
  enum State :String {
    case NOTINITED = "NOTINITED", INITED = "INITED", STARTED = "STARTED" , STOPPED = "STOPPED", INVALID = ""
  }//end state

  enum HAState : String {
    case INITIALIZING = "INITIALIZING", ACTIVE = "ACTIVE", STANDBY = "STANDBY", STOPPED = "STOPPED", INVALID = ""
  }//end HAState

  /// Hadoop common build string with build version, user, and checksum
  var hadoopBuildVersion = ""
  /// Version of hadoop common
  var hadoopVersion = ""
  /// Timestamp when hadoop common was built(in ms since epoch)
  var hadoopVersionBuiltOn = ""
  /// The ResourceManager HA state
  var haState: HAState = .STOPPED
  /// The cluster id
  var id = 0
  /// ResourceManager build string with build version, user, and checksum
  var resourceManagerBuildVersion = ""
  /// Version of the ResourceManager
  var resourceManagerVersion = ""
  /// Timestamp when ResourceManager was built (in ms since epoch)
  var resourceManagerVersionBuiltOn = ""
  /// The time the cluster started (in ms since epoch)
  var startedOn = 0
  /// The ResourceManager state - valid values are: NOTINITED, INITED, STARTED, STOPPED
  var state: State = .STOPPED

  /// constructor of ClusterInfo
  /// - parameters:
  ///   - dictionary: [String:Any], a json decoded dictionary of ClusterInfo
  public init(_ dictionary: [String:Any] = [:]) {
    self.hadoopBuildVersion = dictionary["hadoopBuildVersion"] as? String ?? ""
    self.hadoopVersion = dictionary["hadoopVersion"] as? String ?? ""
    self.hadoopVersionBuiltOn = dictionary["hadoopVersionBuiltOn"] as? String ?? ""
    self.id = dictionary["id"] as? Int ?? 0
    self.resourceManagerBuildVersion = dictionary["resourceManagerBuildVersion"] as? String ?? ""
    self.resourceManagerVersion = dictionary["resourceManagerVersion"] as? String ?? ""
    self.resourceManagerVersionBuiltOn = dictionary["resourceManagerVersionBuiltOn"] as? String ?? ""
    self.startedOn = dictionary["startedOn"] as? Int ?? 0
    self.state = State(rawValue: dictionary["state"] as? String ?? "") ?? .INVALID
    self.haState = HAState(rawValue: dictionary["haState"] as? String ?? "") ?? .INVALID
  }//init
}//Clusterinfo

extension String {
  /// an express way of decoding cluster info from a json string
  public var asClusterInfo: ClusterInfo? {
    get{
      do{
        let dic = try self.jsonDecode() as? [String:Any] ?? [:]
        return ClusterInfo(dic["clusterInfo"] as? [String:Any] ?? [:])
      }catch{
        return nil
      }//end do
    }//end get
  }//end member
}//end extension

/// The cluster metrics resource provides some overall metrics about the cluster. More detailed metrics should be retrieved from the jmx interface.
public struct ClusterMetrics{
  /// The number of active nodes
  var activeNodes = 0
  /// The amount of memory allocated in MB
  var allocatedMB = 0
  /// The number of allocated virtual cores
  var allocatedVirtualCores = 0
  /// The number of applications completed
  var appsCompleted = 0
  /// The number of applications failed
  var appsFailed = 0
  /// The number of applications killed
  var appsKilled = 0
  /// The number of applications pending
  var appsPending = 0
  /// The number of applications running
  var appsRunning = 0
  /// The number of applications submitted
  var appsSubmitted = 0
  /// The amount of memory available in MB
  var availableMB = 0
  /// The number of available virtual cores
  var availableVirtualCores = 0
  /// The number of containers allocated
  var containersAllocated = 0
  /// The number of containers pending
  var containersPending = 0
  /// The number of containers reserved
  var containersReserved = 0
  /// The number of nodes decommissioned
  var decommissionedNodes = 0
  /// The number of lost nodes
  var lostNodes = 0
  /// The number of nodes rebooted
  var rebootedNodes = 0
  /// The amount of memory reserved in MB
  var reservedMB = 0
  /// The number of reserved virtual cores
  var reservedVirtualCores = 0
  /// The amount of total memory in MB
  var totalMB = 0
  /// The total number of nodes
  var totalNodes = 0
  /// The total number of virtual cores
  var totalVirtualCores = 0
  /// The number of unhealthy nodes
  var unhealthyNodes = 0

  /// constructor of ClusterMetrics
  /// - parameters:
  ///   - dictionary: [String:Any], a json decoded dictionary of ClusterMetrics
  public init(_ dictionary: [String:Any] = [:]) {
    self.activeNodes = dictionary["activeNodes"] as? Int ?? 0
    self.allocatedMB = dictionary["allocatedMB"] as? Int ?? 0
    self.allocatedVirtualCores = dictionary["allocatedVirtualCores"] as? Int ?? 0
    self.appsCompleted = dictionary["appsCompleted"] as? Int ?? 0
    self.appsFailed = dictionary["appsFailed"] as? Int ?? 0
    self.appsKilled = dictionary["appsKilled"] as? Int ?? 0
    self.appsPending = dictionary["appsPending"] as? Int ?? 0
    self.appsRunning = dictionary["appsRunning"] as? Int ?? 0
    self.appsSubmitted = dictionary["appsSubmitted"] as? Int ?? 0
    self.availableMB = dictionary["availableMB"] as? Int ?? 0
    self.availableVirtualCores = dictionary["availableVirtualCores"] as? Int ?? 0
    self.containersAllocated = dictionary["containersAllocated"] as? Int ?? 0
    self.containersPending = dictionary["containersPending"] as? Int ?? 0
    self.containersReserved = dictionary["containersReserved"] as? Int ?? 0
    self.decommissionedNodes = dictionary["decommissionedNodes"] as? Int ?? 0
    self.lostNodes = dictionary["lostNodes"] as? Int ?? 0
    self.rebootedNodes = dictionary["rebootedNodes"] as? Int ?? 0
    self.reservedMB = dictionary["reservedMB"] as? Int ?? 0
    self.reservedVirtualCores = dictionary["reservedVirtualCores"] as? Int ?? 0
    self.totalMB = dictionary["totalMB"] as? Int ?? 0
    self.totalNodes = dictionary["totalNodes"] as? Int ?? 0
    self.totalVirtualCores = dictionary["totalVirtualCores"] as? Int ?? 0
    self.unhealthyNodes = dictionary["unhealthyNodes"] as? Int ?? 0
  }//init
}//Clustermetrics

extension String {
  /// an express way of decoding cluster metrics info from a json string
  public var asClusterMetrics: ClusterMetrics? {
    get{
      do{
        let dic = try self.jsonDecode() as? [String:Any] ?? [:]
        return ClusterMetrics(dic["clusterMetrics"] as? [String:Any] ?? [:])
      }catch{
        return nil
      }//end do
    }//end get
  }//end member
}//end extension

/// The ResourceManager allow the user to get information about the cluster - status on the cluster, metrics on the cluster, scheduler information, information about nodes in the cluster, and information about applications on the cluster.
public class YARNResourceManager: YARNNodeManager {

  /// constructor of YARNResourceManager
  /// - parameters:
  ///   - service: the protocol of web request - http / https
  ///		- host: the hostname or ip address of the YARN Resource Manager
  ///		- port: the port of YARN Resource Manager
  ///   - user: the YARN User
  ///		- auth: Authorization Model. Please check the enum Authentication for details
  ///   - proxyUser: proxy user, if applicable
  ///   - extraHeaders: extra headers if special header such as CSRF (Cross-Site Request Forgery Prevention) is applicable
  ///		- apibase: use this parameter ONLY the target server has a different api routine other than /webhdfs/v1
  ///   - timeout: timeout in seconds, zero means never timeout during transfer
  public override init (service: String = "http",
                        host: String = "localhost", port: Int = 8088, user: String = "",
                        auth: Authentication = .off, proxyUser: String = "",
                        extraHeaders: [String] = [],
                        apibase: String = "/ws/v1/cluster", timeout: Int = 0) {
    super.init(service: service, host: host, port: port, auth: auth, proxyUser: proxyUser, extraHeaders: extraHeaders, apibase: apibase, timeout: timeout)
  }//end constructor

  /// The cluster information resource provides overall information about the cluster.
  /// - returns:
  /// ClusterInfo structure, See ClusterInfo.
  /// - throws:
  /// WebHDFS.Exceptions
  public func checkClusterInfo() throws -> ClusterInfo? {
    let (_, dat, _) = try self.perform()
    return dat.asClusterInfo
  }//end func

  /// The cluster metrics resource provides some overall metrics about the cluster. More detailed metrics should be retrieved from the jmx interface
  /// - returns:
  /// ClusterMetrics structure, See ClusterMetrics.
  /// - throws:
  /// WebHDFS.Exceptions
  public func checkClusterMetrics() throws -> ClusterMetrics? {
    let (_, dat, _) = try self.perform(overwriteURL: assembleURL("/metrics"))
    return dat.asClusterMetrics
  }//end func
}//end YARNResourceManager
