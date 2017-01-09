//
//  YARNNodeManager.swift
//  PerfectHadoop - YARN NodeManager Data Structures
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

/// The node information resource provides overall information about that particular node.
public struct NodeInfo{

  /// Hadoop common build string with build version, user, and checksum
  var hadoopBuildVersion = ""
  /// Version of hadoop common
  var hadoopVersion = ""
  /// Timestamp when hadoop common was built(in ms since epoch)
  var hadoopVersionBuiltOn = ""
  /// The diagnostic health report of the node
  var healthReport = ""
  /// The NodeManager id
  var id = ""
  /// The last timestamp at which the health report was received (in ms since epoch)
  var lastNodeUpdateTime = 0
  /// true/false indicator of if the node is healthy
  var nodeHealthy = false
  /// The host name of the NodeManager
  var nodeHostName = ""
  /// NodeManager build string with build version, user, and checksum
  var nodeManagerBuildVersion = ""
  /// Version of the NodeManager
  var nodeManagerVersion = ""
  /// Timestamp when NodeManager was built(in ms since epoch)
  var nodeManagerVersionBuiltOn = ""
  /// The amount of physical memory allocated for use by containers in MB
  var totalPmemAllocatedContainersMB = 0
  /// The number of virtual cores allocated for use by containers
  var totalVCoresAllocatedContainers = 0
  /// The amount of virtual memory allocated for use by containers in MB
  var totalVmemAllocatedContainersMB = 0

  /// constructor
  /// - parameters:
  ///   - dictionary: [String:Any], a dictionary decoded from a json string
  public init(_ dictionary: [String:Any] = [:]) {
    self.hadoopBuildVersion = dictionary["hadoopBuildVersion"] as? String ?? ""
    self.hadoopVersion = dictionary["hadoopVersion"] as? String ?? ""
    self.hadoopVersionBuiltOn = dictionary["hadoopVersionBuiltOn"] as? String ?? ""
    self.healthReport = dictionary["healthReport"] as? String ?? ""
    self.id = dictionary["id"] as? String ?? ""
    self.lastNodeUpdateTime = dictionary["lastNodeUpdateTime"] as? Int ?? 0
    self.nodeHealthy = dictionary["nodeHealthy"] as? Bool ?? false
    self.nodeHostName = dictionary["nodeHostName"] as? String ?? ""
    self.nodeManagerBuildVersion = dictionary["nodeManagerBuildVersion"] as? String ?? ""
    self.nodeManagerVersion = dictionary["nodeManagerVersion"] as? String ?? ""
    self.nodeManagerVersionBuiltOn = dictionary["nodeManagerVersionBuiltOn"] as? String ?? ""
    self.totalPmemAllocatedContainersMB = dictionary["totalPmemAllocatedContainersMB"] as? Int ?? 0
    self.totalVCoresAllocatedContainers = dictionary["totalVCoresAllocatedContainers"] as? Int ?? 0
    self.totalVmemAllocatedContainersMB = dictionary["totalVmemAllocatedContainersMB"] as? Int ?? 0
  }//init
}//Nodeinfo

extension String {
  /// nicely convert a json string directly into a NodeInfo structure
  public var asNodeInfo: NodeInfo? {
    get{
      do{
        let dic = try self.jsonDecode() as? [String:Any] ?? [:]
        return NodeInfo(dic["nodeInfo"] as? [String:Any] ?? [:])
      }catch{
        return nil
      }//end do
    }//end get
  }//end member
}//end extension

/// A container resource contains information about a particular container that is running on this NodeManager.
public struct Container{

  /// valid states
  enum State :String {
    case NEW = "NEW"
    case LOCALIZING = "LOCALIZING"
    case LOCALIZATION_FAILED = "LOCALIZATION_FAILED"
    case LOCALIZED = "LOCALIZED"
    case RUNNING = "RUNNING"
    case EXITED_WITH_SUCCESS = "EXITED_WITH_SUCCESS"
    case EXITED_WITH_FAILURE = "EXITED_WITH_FAILURE"
    case KILLING = "KILLING"
    case CONTAINER_CLEANEDUP_AFTER_KILL = "CONTAINER_CLEANEDUP_AFTER_KILL"
    case CONTAINER_RESOURCES_CLEANINGUP = "CONTAINER_RESOURCES_CLEANINGUP"
    case DONE = "DONE"
    case INVALID = ""
  }//end enum

  /// The http link to the container logs
  var containerLogsLink = ""
  /// A diagnostic message for failed containers
  var diagnostics = ""
  /// Exit code of the container
  var exitCode = 0
  /// The container id
  var id = ""
  /// The id of the node the container is on
  var nodeId = ""
  /// State of the container
  var state: State = .INVALID
  /// Total amout of memory needed by the container (in MB)
  var totalMemoryNeededMB = 0
  /// Total number of virtual cores needed by the container
  var totalVCoresNeeded = 0
  /// The user name of the user which started the container
  var user = ""
  /// constructor
  /// - parameters:
  ///   - dictionary: [String:Any], a dictionary decoded from a json string
  public init(_ dictionary: [String:Any] = [:]) {
    self.containerLogsLink = dictionary["containerLogsLink"] as? String ?? ""
    self.diagnostics = dictionary["diagnostics"] as? String ?? ""
    self.exitCode = dictionary["exitCode"] as? Int ?? 0
    self.id = dictionary["id"] as? String ?? ""
    self.nodeId = dictionary["nodeId"] as? String ?? ""
    self.state = State(rawValue: dictionary["state"] as? String ?? "") ?? .INVALID
    self.totalMemoryNeededMB = dictionary["totalMemoryNeededMB"] as? Int ?? 0
    self.totalVCoresNeeded = dictionary["totalVCoresNeeded"] as? Int ?? 0
    self.user = dictionary["user"] as? String ?? ""
  }//init
}//Container

extension String {
  /// nicely convert a json string directly into a Container structure
  public var asContainer: Container? {
    get{
      do{
        let dic = try self.jsonDecode() as? [String:Any] ?? [:]
        return Container (dic["container"] as? [String:Any] ?? [:])
      }catch{
        return nil
      }//end do
    }//end get
  }//end member
  
  /// nicely convert a json string directly into an array of Containers
  public var asContainers: [Container] {
    get{
      do{
        let dic = try self.jsonDecode() as? [String:Any] ?? [:]
        let c = dic["containers"] as? [String:Any] ?? [:]
        return (c["container"] as? [Any] ?? []).map { Container ($0 as? [String:Any] ?? [:])}
      }catch{
        return []
      }//end do
    }//end get
  }//end member
}//end extension

/// The NodeManager allow the user to get status on the node and information about applications and containers running on that node.
public class YARNNodeManager: WebHDFS {

  /// constructor of YARNNode
  /// - parameters:
  ///   - service: the protocol of web request - http / https
  ///		- host: the hostname or ip address of the YARN node host
  ///		- port: the port of YARN node
  ///   - user: the YARN User
  ///		- auth: Authorization Model. Please check the enum Authentication for details
  ///   - proxyUser: proxy user, if applicable
  ///   - extraHeaders: extra headers if special header such as CSRF (Cross-Site Request Forgery Prevention) is applicable
  ///		- apibase: use this parameter ONLY the target server has a different api routine other than /webhdfs/v1
  ///   - timeout: timeout in seconds, zero means never timeout during transfer
  public override init (service: String = "http",
                        host: String = "localhost", port: Int = 8042, user: String = "",
                        auth: Authentication = .off, proxyUser: String = "",
                        extraHeaders: [String] = [],
                        apibase: String = "/ws/v1/node", timeout: Int = 0) {
    super.init(service: service, host: host, port: port, auth: auth, proxyUser: proxyUser, extraHeaders: extraHeaders, apibase: apibase, timeout: timeout)
  }//end constructor

  /// generate a url by the provided information
  /// - returns:
  ///	the assembled url
  ///	- parameters:
  ///		- operation: each webhdfs API has a unique operation string, such as CREATE / GETFILESTATUS, etc.
  ///		- path: full path of the objective file / directory.
  ///		- variables: further options to complete this operation
  @discardableResult
  internal func assembleURL(_ path: String) -> String {
    // assamble the url path
    return "\(service)://\(host):\(port)\(base)\(path)"
  }//end assembleURL

  /// Check the node information resource provides overall information about that particular node.
  /// - returns:
  /// NodeInfo structure, See NodeInfo.
  /// - throws:
  /// WebHDFS.Exceptions
  @discardableResult
  public func checkOverall() throws -> NodeInfo? {
    let (_, dat, _) = try self.perform()
    return dat.asNodeInfo
  }//end func

  /// With the Applications API, you can obtain a collection of resources, each of which represents an application. When you run a GET operation on this resource, you obtain a collection of Application Objects.
  /// - returns:
  /// An array of APP structure. See APP.
  /// - throws:
  /// WebHDFS.Exceptions
  @discardableResult
  public func checkApps() throws -> [APP] {
    let (_, dat, _) = try self.perform(overwriteURL: assembleURL("/apps"))
    return dat.asApps
  }//end func

  /// An application resource contains information about a particular application that was run or is running on this NodeManager.
  /// - returns:
  /// An APP struct. See APP.
  /// - throws:
  /// WebHDFS.Exceptions
  @discardableResult
  public func checkApp(id: String) throws -> APP? {
    let (_, dat, _) = try self.perform(overwriteURL: assembleURL("/apps/\(id)"))
    return dat.asApp
  }//end func

  /// When you make a request for the list of containers, the information will be returned as collection of container objects.
  /// - returns:
  /// An array of Container. See Container.
  /// - throws:
  /// WebHDFS.Exceptions
  @discardableResult
  public func checkContainers() throws -> [Container] {
    let (_, dat, _) = try self.perform(overwriteURL: assembleURL("/containers"))
    return dat.asContainers
  }//end func

  /// A container resource contains information about a particular container that is running on this NodeManager.
  /// - returns:
  /// Container data structure. See Container.
  /// - throws:
  /// WebHDFS.Exceptions
  @discardableResult
  public func checkContainer(id: String) throws -> Container? {
    let (_, dat, _) = try self.perform(overwriteURL: assembleURL("/containers/\(id)"))
    return dat.asContainer
  }//end func
}//end class
