//
//  MapReduceHistory.swift
//  PerfectHadoop
//
//  Created by Rocky Wei on 2017-01-11.
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
import cURL
import PerfectLib

/// History Info when check an overall info from a history server.
public struct HistoryInfo {
  /// Hadoop common build string with build version, user, and checksum
  var hadoopBuildVersion = ""
  /// Version of hadoop common
  var hadoopVersion = ""
  /// Timestamp when hadoop common was built
  var hadoopVersionBuiltOn = ""
  /// The time the history server was started (in ms since epoch)
  var startedOn = 0
  /// constructor
  public init(_ dictionary: [String:Any] = [:]){
    self.hadoopBuildVersion = dictionary["hadoopBuildVersion"] as? String ?? ""
    self.hadoopVersion = dictionary["hadoopVersion"] as? String ?? ""
    self.hadoopVersionBuiltOn = dictionary["hadoopVersionBuiltOn"] as? String ?? ""
    self.startedOn = dictionary["startedOn"] as? Int ?? 0
  }//init
}//Historyinfo

extension String {
  public var asHistoryInfo: HistoryInfo? {
    get{
      do{
        let dic = try self.jsonDecode() as? [String:Any] ?? [:]
        return HistoryInfo(dic["historyInfo"] as? [String:Any] ?? [:])
      }catch{
        return nil
      }//end do
    }//end get
  }//end member
}//end extension


/// The history server information resource provides overall information about the history server.
public class MapReduceHistroy: YARNResourceManager {

  /// constructor of MapReduceHistroy
  /// - parameters:
  ///   - service: the protocol of web request - http / https
  ///		- host: the hostname or ip address of the MapReduce History Server
  ///		- port: the port of MapReduce History Server
  ///   - user: the MapReduce User
  ///		- auth: Authorization Model. Please check the enum Authentication for details
  ///   - proxyUser: proxy user, if applicable
  ///   - extraHeaders: extra headers if special header such as CSRF (Cross-Site Request Forgery Prevention) is applicable
  ///		- apibase: use this parameter ONLY the target server has a different api routine other than /webhdfs/v1
  ///   - timeout: timeout in seconds, zero means never timeout during transfer
  public override init (service: String = "http",
                        host: String = "localhost", port: Int = 19888, user: String = "",
                        auth: Authentication = .off, proxyUser: String = "",
    apibase: String = "/ws/v1/history", timeout: Int = 0) {
    super.init(service: service, host: host, port: port, user: user, auth: auth, proxyUser: proxyUser,
      apibase: apibase, timeout: timeout)
  }//end constructor

  /// The history server information resource provides overall information about the history server.
  /// - returns:
  /// HistoryInfo structure, See HistoryInfo.
  /// - throws:
  /// WebHDFS.Exceptions
  public func info() throws -> HistoryInfo? {
    let url = assembleURL("")
    let (_, dat, _) = try self.perform(overwriteURL: url)
    return dat.asHistoryInfo
  }//end checkOverall
}//end MapReduceHistory
