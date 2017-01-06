//
//  YARNSJobTaskAttemptCounters.swift
//  PerfectHadoop
//
//  Created by Rockford Wei on 2016-01-04.
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


public struct JobTaskAttemptCounters {

  public enum Exception: Error {
    case CAST
  }//end Exception

  public struct Counter{
    var name = ""
    var value = 0
    public init(_ dictionary: [String:Any] = [:]) {
      self.name = dictionary["name"] as? String ?? ""
      self.value = dictionary["value"] as? Int ?? 0
    }//init
  }//counterItem

  public struct FileSystem {
    public static let counterGroupName = "org.apache.hadoop.mapreduce.FileSystemCounter"
    public var FILE_BYTES_READ = 0
    public var FILE_BYTES_WRITTEN = 0
    public var FILE_READ_OPS = 0
    public var FILE_LARGE_READ_OPS = 0
    public var HDFS_BYTES_READ = 0
    public var HDFS_BYTES_OPS = 0
    public var HDFS_LARGE_READ_OPS = 0
    public var HDFS_WRITE_OPS = 0
    public init(_ dictionary: [String:Any] = [:]) throws {
      guard let groupName = dictionary["counterGroupName"] as? String else { throw Exception.CAST }
      guard groupName == FileSystem.counterGroupName else { throw Exception.CAST }

      let counters = (dictionary["counter"] as? [Any] ?? []).map{Counter($0 as? [String : Any] ?? [:])}
      counters.forEach { counter in
        switch(counter.name) {
        case "FILE_BYTES_READ":
          self.FILE_BYTES_READ = counter.value
        case "FILE_BYTES_WRITTEN":
          self.FILE_BYTES_WRITTEN = counter.value
        case "FILE_READ_OPS":
          self.FILE_READ_OPS = counter.value
        case "FILE_LARGE_READ_OPS":
          self.FILE_LARGE_READ_OPS = counter.value
        case "HDFS_BYTES_READ":
          self.HDFS_BYTES_READ = counter.value
        case "HDFS_BYTES_OPS":
          self.HDFS_BYTES_OPS = counter.value
        case "HDFS_LARGE_READ_OPS":
          self.HDFS_LARGE_READ_OPS = counter.value
        case "HDFS_WRITE_OPS":
          self.HDFS_WRITE_OPS = counter.value
        default:
          ()
        }//end case
      }//next
    }//end constructor
  }//end FileSystem

  public struct Task {
    public static let counterGroupName = "org.apache.hadoop.mapreduce.TaskCounter"
    public var COMBINE_INPUT_RECORDS = 0
    public var COMBINE_OUTPUT_RECORDS = 0
    public var REDUCE_INPUT_GROUPS = 0
    public var REDUCE_SHUFFLE_BYTES = 0
    public var REDUCE_INPUT_RECORDS = 0
    public var REDUCE_OUTPUT_RECORDS = 0
    public var SPILLED_RECORDS = 0
    public var SHUFFLED_MAPS = 0
    public var FAILED_SHUFFLE = 0
    public var MERGED_MAP_OUTPUTS = 0
    public var GC_TIME_MILLIS = 0
    public var CPU_MILLISECONDS = 0
    public var PHYSICAL_MEMORY_BYTES = 0
    public var VIRTUAL_MEMORY_BYTES = 0
    public var COMMITTED_HEAP_BYTES = 0
    public init(_ dictionary: [String:Any] = [:]) throws {
      guard let groupName = dictionary["counterGroupName"] as? String else { throw Exception.CAST }
      guard groupName == Task.counterGroupName else { throw Exception.CAST }
      let counters = (dictionary["counter"] as? [Any] ?? []).map{Counter($0 as? [String : Any] ?? [:])}
      counters.forEach { counter in
        switch(counter.name) {
        case "COMBINE_INPUT_RECORDS":
          self.COMBINE_INPUT_RECORDS = counter.value
        case "COMBINE_OUTPUT_RECORDS":
          self.COMBINE_OUTPUT_RECORDS = counter.value
        case "REDUCE_INPUT_GROUPS":
          self.REDUCE_INPUT_GROUPS = counter.value
        case "REDUCE_SHUFFLE_BYTES":
          self.REDUCE_SHUFFLE_BYTES = counter.value
        case "REDUCE_INPUT_RECORDS":
          self.REDUCE_INPUT_RECORDS = counter.value
        case "REDUCE_OUTPUT_RECORDS":
          self.REDUCE_OUTPUT_RECORDS = counter.value
        case "SPILLED_RECORDS":
          self.SPILLED_RECORDS = counter.value
        case "SHUFFLED_MAPS":
          self.SHUFFLED_MAPS = counter.value
        case "FAILED_SHUFFLE":
          self.FAILED_SHUFFLE = counter.value
        case "MERGED_MAP_OUTPUTS":
          self.MERGED_MAP_OUTPUTS = counter.value
        case "GC_TIME_MILLIS":
          self.GC_TIME_MILLIS = counter.value
        case "CPU_MILLISECONDS":
          self.CPU_MILLISECONDS = counter.value
        case "PHYSICAL_MEMORY_BYTES":
          self.PHYSICAL_MEMORY_BYTES = counter.value
        case "VIRTUAL_MEMORY_BYTES":
          self.VIRTUAL_MEMORY_BYTES = counter.value
        case "COMMITTED_HEAP_BYTES":
          self.COMMITTED_HEAP_BYTES = counter.value
        default:
          ()
        }//end case
      }//next
    }//end constructor
  }//end struct

  public struct ShuffleErrors {
    public static let counterGroupName = "Shuffle Errors"
    public var BAD_ID = 0
    public var CONNECTION = 0
    public var IO_ERROR = 0
    public var WRONG_LENGTH = 0
    public var WRONG_MAP = 0
    public var WRONG_REDUCE = 0
    public init(_ dictionary: [String:Any] = [:]) throws {
      guard let groupName = dictionary["counterGroupName"] as? String else { throw Exception.CAST }
      guard groupName == ShuffleErrors.counterGroupName else { throw Exception.CAST }
      let counters = (dictionary["counter"] as? [Any] ?? []).map{Counter($0 as? [String : Any] ?? [:])}
      counters.forEach { counter in
        switch(counter.name) {
        case "BAD_ID":
          self.BAD_ID = counter.value
        case "CONNECTION":
          self.CONNECTION = counter.value
        case "IO_ERROR":
          self.IO_ERROR = counter.value
        case "WRONG_LENGTH":
          self.WRONG_LENGTH = counter.value
        case "WRONG_MAP":
          self.WRONG_MAP = counter.value
        case "WRONG_REDUCE":
          self.WRONG_REDUCE = counter.value
        default:
          ()
        }//end case
      }//next
    }//end constructor
  }//end struct

  public struct FileOutputFormat {
    public static let counterGroupName = "org.apache.hadoop.mapreduce.lib.output.FileOutputFormatCounter"
    public var BYTES_WRITTEN = 0
    public init(_ dictionary: [String:Any] = [:]) throws {
      guard let groupName = dictionary["counterGroupName"] as? String else { throw Exception.CAST }
      guard groupName == FileOutputFormat.counterGroupName else { throw Exception.CAST }
      let counters = (dictionary["counter"] as? [Any] ?? []).map{Counter($0 as? [String : Any] ?? [:])}
      counters.forEach { counter in
        switch(counter.name) {
        case "BYTES_WRITTEN":
          self.BYTES_WRITTEN = counter.value
        default:
          ()
        }//end case
      }//next
    }//end constructor
  }//end struct

  public var id = ""
  public var fileSystem: FileSystem?
  public var task: Task?
  public var fileOutputFormat: FileOutputFormat?
  public var shuffleErrors: ShuffleErrors?

  public init(_ dictionary:[String:Any] = [:]) throws {
    guard let id = dictionary["id"] as? String else {
      throw Exception.CAST
    }//end guard
    self.id = id
    guard let group = dictionary["taskAttemptCounterGroup"] as? [[String:Any]] else {
      throw Exception.CAST
    }//end guard
    group.forEach{ item in
      guard let groupName = item["counterGroupName"] as? String else { return }
      do {
        switch(groupName) {
        case FileSystem.counterGroupName:
          self.fileSystem = try FileSystem(item)
        case Task.counterGroupName:
          self.task = try Task(item)
        case FileOutputFormat.counterGroupName:
          self.fileOutputFormat = try FileOutputFormat(item)
        case ShuffleErrors.counterGroupName:
          self.shuffleErrors = try ShuffleErrors(item)
        default:
          ()
        }
      }catch {}
    }//next
  }//end constructor

  public init (json: String) throws {
    let dic = try json.jsonDecode() as? [String:Any] ?? [:]
    try self.init(dic)
  }//end init
}//end JobTaskAttemptCounters

extension String {
  public var asJobTaskAttemptCounters: JobTaskAttemptCounters? {
    get {
      do {
        let dic = try self.jsonDecode() as? [String:Any] ?? [:]
        return try JobTaskAttemptCounters(dic["JobTaskAttemptCounters"] as? [String:Any] ?? [:])
      }catch {
        return nil
      }//end do
    }//end get
  }//end member
}//end extension

public struct Task {
  var state = ""
  var id = ""
  var progress = 0
  var finishTime = 0
  var elapsedTime = 0
  var startTime = 0
  var type = ""
  var successfulAttempt = ""
  public init(_ dictionary: [String:Any] = [:]) {
    self.state = dictionary["state"] as? String ?? ""
    self.id = dictionary["id"] as? String ?? ""
    self.progress = dictionary["progress"] as? Int ?? 0
    self.finishTime = dictionary["finishTime"] as? Int ?? 0
    self.elapsedTime = dictionary["elapsedTime"] as? Int ?? 0
    self.startTime = dictionary["startTime"] as? Int ?? 0
    self.type = dictionary["type"] as? String ?? ""
    self.successfulAttempt = dictionary["successfulAttempt"] as? String ?? ""
  }//init
}//task

extension String {
  public var asTasks: [Task] {
    get {
      do {
        let dic = try self.jsonDecode() as? [String:Any] ?? [:]
        let tasks = dic["tasks"] as? [String: Any] ?? [:]
        return (tasks["task"] as? [Any] ?? []).map {Task($0 as? [String:Any] ?? [:])}
      }catch {
        return []
      }//end do
    }//end get
  }//end member
}//end extension

public struct TaskAttempt {
  var state = ""
  var nodeHttpAddress = ""
  var elapsedShuffleTime = 0
  var mergeFinishTime = 0
  var finishTime = 0
  var elapsedTime = 0
  var elapsedMergeTime = 0
  var type = ""
  var assignedContainerId = ""
  var rack = ""
  var shuffleFinishTime = 0
  var id = ""
  var progress = ""
  var elapsedReduceTime = 0
  var startTime = 0
  public init(_ dictionary: [String:Any] = [:]) {
    self.state = dictionary["state"] as? String ?? ""
    self.nodeHttpAddress = dictionary["nodeHttpAddress"] as? String ?? ""
    self.elapsedShuffleTime = dictionary["elapsedShuffleTime"] as? Int ?? 0
    self.mergeFinishTime = dictionary["mergeFinishTime"] as? Int ?? 0
    self.finishTime = dictionary["finishTime"] as? Int ?? 0
    self.elapsedTime = dictionary["elapsedTime"] as? Int ?? 0
    self.elapsedMergeTime = dictionary["elapsedMergeTime"] as? Int ?? 0
    self.type = dictionary["type"] as? String ?? ""
    self.assignedContainerId = dictionary["assignedContainerId"] as? String ?? ""
    self.rack = dictionary["rack"] as? String ?? ""
    self.shuffleFinishTime = dictionary["shuffleFinishTime"] as? Int ?? 0
    self.id = dictionary["id"] as? String ?? ""
    self.progress = dictionary["progress"] as? String ?? ""
    self.elapsedReduceTime = dictionary["elapsedReduceTime"] as? Int ?? 0
    self.startTime = dictionary["startTime"] as? Int ?? 0
  }//init
}//task

extension String {
  public var asTaskAttemps: [TaskAttempt] {
    get {
      do {
        let dic = try self.jsonDecode() as? [String:Any] ?? [:]
        let tasks = dic["taskAttempts"] as? [String: Any] ?? [:]
        return (tasks["taskAttempt"] as? [Any] ?? []).map {TaskAttempt($0 as? [String:Any] ?? [:])}
      }catch {
        return []
      }//end do
    }//end get
  }//end member
}//end extension


public struct Job{
  public struct ACL{
    var name = ""
    var value = ""
    public init(_ dictionary: [String:Any] = [:]) {
      self.name = dictionary["name"] as? String ?? ""
      self.value = dictionary["value"] as? String ?? ""
    }//init
  }//ACL

  var state = ""
  var mapsCompleted = 0
  var newMapAttempts = 0
  var finishTime = 0
  var reducesPending = 0
  var reducesTotal = 0
  var runningMapAttempts = 0
  var elapsedTime = 0
  var reducesCompleted = 0
  var mapProgress = 0
  var successfulReduceAttempts = 0
  var reducesRunning = 0
  var id = ""
  var runningReduceAttempts = 0
  var name = ""
  var user = ""
  var uberized = false
  var mapsPending = 0
  var failedMapAttempts = 0
  var killedMapAttempts = 0
  var successfulMapAttempts = 0
  var reduceProgress: Double = 0
  var newReduceAttempts = 0
  var killedReduceAttempts = 0
  var acls : [ACL] = []
  var mapsRunning = 0
  var mapsTotal = 0
  var diagnostics = ""
  var failedReduceAttempts = 0
  var startTime = 0
  var avgShuffleTime = 0
  var queue = ""
  var avgMergeTime = 0
  var avgMapTime = 0
  var avgReduceTime = 0

  public init(_ dictionary: [String:Any] = [:]) {
    self.avgReduceTime = dictionary["avgReduceTime"] as? Int ?? 0
    self.avgMapTime = dictionary["avgMapTime"] as? Int ?? 0
    self.avgMergeTime = dictionary["avgMergeTime"] as? Int ?? 0
    self.queue = dictionary["queue"] as? String ?? ""
    self.avgShuffleTime = dictionary["avgShuffleTime"] as? Int ?? 0
    self.state = dictionary["state"] as? String ?? ""
    self.mapsCompleted = dictionary["mapsCompleted"] as? Int ?? 0
    self.newMapAttempts = dictionary["newMapAttempts"] as? Int ?? 0
    self.finishTime = dictionary["finishTime"] as? Int ?? 0
    self.reducesPending = dictionary["reducesPending"] as? Int ?? 0
    self.reducesTotal = dictionary["reducesTotal"] as? Int ?? 0
    self.runningMapAttempts = dictionary["runningMapAttempts"] as? Int ?? 0
    self.elapsedTime = dictionary["elapsedTime"] as? Int ?? 0
    self.reducesCompleted = dictionary["reducesCompleted"] as? Int ?? 0
    self.mapProgress = dictionary["mapProgress"] as? Int ?? 0
    self.successfulReduceAttempts = dictionary["successfulReduceAttempts"] as? Int ?? 0
    self.reducesRunning = dictionary["reducesRunning"] as? Int ?? 0
    self.id = dictionary["id"] as? String ?? ""
    self.runningReduceAttempts = dictionary["runningReduceAttempts"] as? Int ?? 0
    self.name = dictionary["name"] as? String ?? ""
    self.user = dictionary["user"] as? String ?? ""
    self.uberized = dictionary["uberized"] as? Bool ?? false
    self.mapsPending = dictionary["mapsPending"] as? Int ?? 0
    self.failedMapAttempts = dictionary["failedMapAttempts"] as? Int ?? 0
    self.killedMapAttempts = dictionary["killedMapAttempts"] as? Int ?? 0
    self.successfulMapAttempts = dictionary["successfulMapAttempts"] as? Int ?? 0
    self.reduceProgress = dictionary["reduceProgress"] as? Double ?? 0.0
    self.newReduceAttempts = dictionary["newReduceAttempts"] as? Int ?? 0
    self.killedReduceAttempts = dictionary["killedReduceAttempts"] as? Int ?? 0
    self.acls = (dictionary["acls"] as? [Any] ?? []).map{ACL($0 as? [String : Any] ?? [:])}
    self.mapsRunning = dictionary["mapsRunning"] as? Int ?? 0
    self.mapsTotal = dictionary["mapsTotal"] as? Int ?? 0
    self.diagnostics = dictionary["diagnostics"] as? String ?? ""
    self.failedReduceAttempts = dictionary["failedReduceAttempts"] as? Int ?? 0
    self.startTime = dictionary["startTime"] as? Int ?? 0
  }//init
}//Job

extension String {
  public var asJobs: [Job] {
    get {
      do {
        let dic = try self.jsonDecode() as? [String:Any] ?? [:]
        let jobs = dic["jobs"] as? [String: Any] ?? [:]
        return (jobs["job"] as? [Any] ?? []).map {Job($0 as? [String:Any] ?? [:])}
      }catch {
        return []
      }//end do
    }//end get
  }//end member
  public var asJob: Job? {
    get {
      do {
        let dic = try self.jsonDecode() as? [String:Any] ?? [:]
        return Job(dic["job"] as? [String: Any] ?? [:])
      }catch {
        return nil
      }//end do
    }//end get
  }//end member
}//end extension
/*
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
 return []
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
 
 */
