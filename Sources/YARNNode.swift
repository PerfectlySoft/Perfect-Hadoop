//
//  YARNNode.swift
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

public struct NodeInfo{
  var hadoopBuildVersion = ""
  var hadoopVersion = ""
  var hadoopVersionBuiltOn = ""
  var healthReport = ""
  var id = ""
  var lastNodeUpdateTime = 0
  var nodeHealthy = false
  var nodeHostName = ""
  var nodeManagerBuildVersion = ""
  var nodeManagerVersion = ""
  var nodeManagerVersionBuiltOn = ""
  var totalPmemAllocatedContainersMB = 0
  var totalVCoresAllocatedContainers = 0
  var totalVmemAllocatedContainersMB = 0
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

public struct Container{
  var containerLogsLink = ""
  var diagnostics = ""
  var exitCode = 0
  var id = ""
  var nodeId = ""
  var state = ""
  var totalMemoryNeededMB = 0
  var totalVCoresNeeded = 0
  var user = ""
  public init(_ dictionary: [String:Any] = [:]) {
    self.containerLogsLink = dictionary["containerLogsLink"] as? String ?? ""
    self.diagnostics = dictionary["diagnostics"] as? String ?? ""
    self.exitCode = dictionary["exitCode"] as? Int ?? 0
    self.id = dictionary["id"] as? String ?? ""
    self.nodeId = dictionary["nodeId"] as? String ?? ""
    self.state = dictionary["state"] as? String ?? ""
    self.totalMemoryNeededMB = dictionary["totalMemoryNeededMB"] as? Int ?? 0
    self.totalVCoresNeeded = dictionary["totalVCoresNeeded"] as? Int ?? 0
    self.user = dictionary["user"] as? String ?? ""
  }//init
}//Container

extension String {
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

public class YARNNode: WebHDFS {

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

  public func checkOverall() throws -> NodeInfo? {
    let (_, dat, _) = try self.perform()
    return dat.asNodeInfo
  }//end func

  public func checkApps() throws -> [APP] {
    let (_, dat, _) = try self.perform(overwriteURL: assembleURL("/apps"))
    return dat.asApps
  }//end func

  public func checkApp(id: String) throws -> APP? {
    let (_, dat, _) = try self.perform(overwriteURL: assembleURL("/apps/\(id)"))
    return dat.asApp
  }//end func

  public func checkContainers() throws -> [Container] {
    let (_, dat, _) = try self.perform(overwriteURL: assembleURL("/containers"))
    return dat.asContainers
  }//end func

  public func checkContainer(id: String) throws -> Container? {
    let (_, dat, _) = try self.perform(overwriteURL: assembleURL("/containers/\(id)"))
    return dat.asContainer
  }
}//end class
