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

/// The jobs resource provides a list of the MapReduce jobs.
public struct Job{
  public struct ACL{
    var name = ""
    var value = ""
    public init(_ dictionary: [String:Any] = [:]) {
      self.name = dictionary["name"] as? String ?? ""
      self.value = dictionary["value"] as? String ?? ""
    }//init
  }//ACL

  /// the job state
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
  /// user name
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
  /// queue name
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

/// With the job attempts API, you can obtain a collection of resources that represent a job attempt. When you run a GET operation on this resource, you obtain a collection of Job Attempt Objects.
public struct JobAttempt{
  /// The id of the container for the job attempt
  var containerId = ""
  /// The job attempt id
  var id = 0
  /// The http link to the job attempt logs
  var logsLink = ""
  /// The node http address of the node the attempt ran on
  var nodeHttpAddress = ""
  /// The node id of the node the attempt ran on
  var nodeId = ""
  /// The node id of the node the attempt ran on
  var startTime = 0
  /// constructor of JobAttempt
  /// - parameters:
  /// A dictionary decoded from a json string
  public init(_ dictionary: [String:Any] = [:]) {
    self.containerId = dictionary["containerId"] as? String ?? ""
    self.id = dictionary["id"] as? Int ?? 0
    self.logsLink = dictionary["logsLink"] as? String ?? ""
    self.nodeHttpAddress = dictionary["nodeHttpAddress"] as? String ?? ""
    self.nodeId = dictionary["nodeId"] as? String ?? ""
    self.startTime = dictionary["startTime"] as? Int ?? 0
  }//init
}//jobAttemptItem

extension String {
  public var asJobAttempts: [JobAttempt] {
    get {
      do {
        let dic = try self.jsonDecode() as? [String:Any] ?? [:]
        let jobs = dic["jobAttempts"] as? [String: Any] ?? [:]
        return (jobs["jobAttempt"] as? [Any] ?? []).map {JobAttempt($0 as? [String:Any] ?? [:])}
      }catch {
        return []
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
  @discardableResult
  public func info() throws -> HistoryInfo? {
    let url = assembleURL("")
    let (_, dat, _) = try self.perform(overwriteURL: url)
    return dat.asHistoryInfo
  }//end checkOverall

  /// The jobs resource provides a list of the MapReduce jobs that have finished. It does not currently return a full list of parameters
  /// - parameters:
  ///   - finishedTimeBegin: jobs with finish time beginning with this time, specified in ms since epoch
  ///   - finishedTimeEnd: jobs with finish time ending with this time, specified in ms since epoch
  ///   - startedTimeBegin: jobs with start time beginning with this time, specified in ms since epoch
  ///   - startedTimeEnd: jobs with start time ending with this time, specified in ms since epoch
  ///   - limit: total number of app objects to be returned
  ///   - user: user name
  ///   - state: the job state, see APP.State
  ///   - queue: queue name
  /// - throws
  /// WebHDFS.Exceptions
  /// - returns:
  /// [Job], an array of Job Structures
  @discardableResult
  public func checkJobs(user: String? = nil, state:APP.FinalStatus? = nil, queue: String? = nil, limit: Int? = nil, startedTimeBegin: Int? = nil, startedTimeEnd: Int? = nil, finishedTimeBegin: Int? = nil, finishedTimeEnd: Int? = nil) throws -> [Job] {
    var url = assembleURL("/mapreduce/jobs")
    var v: [String: Any] = [:]
    if user != nil {
      v["user"] = user ?? ""
    }//end if
    if state != nil {
      v["state"] = state ?? .INVALID
    }//end if
    if queue != nil {
      v["queue"] = queue ?? ""
    }//end if
    if limit != nil {
      v["limit"] = limit ?? 10
    }//end if
    if startedTimeBegin != nil {
      v["startedTimeBegin"] = startedTimeBegin ?? 0
    }//end if
    if startedTimeEnd != nil {
      v["startedTimeEnd"] = startedTimeEnd ?? 0
    }//end if

    if finishedTimeBegin != nil {
      v["finishedTimeBegin"] = finishedTimeBegin ?? 0
    }//end if

    if finishedTimeEnd != nil {
      v["finishedTimeEnd"] = finishedTimeEnd ?? 0
    }//end if

    if v.count > 0 {
      let x = v.reduce("?") { previous, next in

        let exp = "\(next.key)=\(next.value)".stringByEncodingURL
        if previous == "?" {
          return previous + exp
        }//end if
        return previous + "&" + exp
      }//end x
      url.append(x)
    }//end if
    let (_, dat, _) = try self.perform(overwriteURL: url)
    return dat.asJobs
  }//end func

  /// With the job attempts API, you can obtain a collection of resources that represent a job attempt. When you run a GET operation on this resource, you obtain a collection of Job Attempt Objects.
  /// - parameters:
  ///   - jobId: the job's id to check
  /// - throws
  /// WebHDFS.Exceptions
  /// - returns:
  /// [JobAttempt], an array of JobAttemp structures
  @discardableResult
  public func checkJobAttempts(jobId: String) throws -> [JobAttempt] {
    guard !jobId.isEmpty else {
      throw Exception.insufficientParameters
    }//end guard
    let url = assembleURL("/mapreduce/jobs/\(jobId)/jobattempts")
    let (_, dat, _) = try self.perform(overwriteURL: url)
    return dat.asJobAttempts
  }//end func
}//end MapReduceHistory