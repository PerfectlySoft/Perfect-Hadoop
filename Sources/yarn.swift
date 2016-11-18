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
  private var yarnBase: String = "http://localhost:8088/ws/v1/cluster/apps"

  public init(yarnBase: String = "http://localhost:8088/ws/v1/cluster/apps") {
    self.yarnBase = yarnBase
  }//end init

  public func appStatus(appId: String) throws -> [String:Any]{
    let url = "\(yarnBase)/\(appId)"
    print(url)
    let (header, body, res, _) = try perform(overwriteURL: url)
    guard let app = res["app"] as? [String:Any] else {
      throw Exception.unexpectedResponse(url: url, header: header, body: body)
    }//end guard
    return app
  }
}
