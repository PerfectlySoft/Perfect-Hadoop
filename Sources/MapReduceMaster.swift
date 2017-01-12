//
//  MapReduceMaster.swift
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

/// The MapReduce Application Master REST API’s allow the user to get status on the running MapReduce application master. Currently this is the equivalent to a running MapReduce job. The information includes the jobs the app master is running and all the job particulars like tasks, counters, configuration, attempts, etc. The application master should be accessed via the proxy. This proxy is configurable to run either on the resource manager or on a separate host. The proxy URL usually looks like: http://<proxy http address:port>/proxy/appid.
public class MapReduceMaster: YARNNodeManager {

  /// constructor of MapReduceMaster
  /// - parameters:
  ///   - service: the protocol of web request - http / https
  ///		- host: the hostname or ip address of the Mapreduce Application Proxy
  ///		- port: the port of the Mapreduce Application Proxy
  ///   - user: the the Mapreduce Application Proxy User
  ///		- auth: Authorization Model. Please check the enum Authentication for details
  ///   - proxyUser: proxy user, if applicable
  ///		- apibase: use this parameter ONLY the target server has a different api routine other than /proxy
  ///   - timeout: timeout in seconds, zero means never timeout during transfer
  public override init (service: String = "http",
                        host: String = "localhost", port: Int = 8088, user: String = "",
                        auth: Authentication = .off, proxyUser: String = "",
                        apibase: String = "/proxy", timeout: Int = 0) {
    super.init(service: service, host: host, port: port, user: user, auth: auth, proxyUser: proxyUser,
               apibase: apibase, timeout: timeout)
  }//end constructor

  /// generate a url by the provided information
  /// - returns:
  ///	the assembled url
  ///	- parameters:
  ///		- appId: application id of the mapreduce app
  ///		- path: full path of the url
  @discardableResult
  internal func assembleURL(appId: String, path: String) -> String {
    // assamble the url path
    return "\(service)://\(host):\(port)\(base)/\(appId)/ws/v1/mapreduce\(path)"
}//end assembleURL
}//end class
