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

/// The history server information resource provides overall information about the history server.
public class MapReduceHistroy: YARNResourceManager {

  /// History Info when check an overall info from a history server.
  public struct Info {

    /// Hadoop common build string with build version, user, and checksum
    var hadoopBuildVersion = ""

    /// Version of hadoop common
    var hadoopVersion = ""

    /// Timestamp when hadoop common was built
    var hadoopVersionBuiltOn = ""

    /// The time the history server was started (in ms since epoch)
    var startedOn = 0

    /// constructor
    /// - parameters:
    ///   a dictionary decoded from a json string
    public init(_ dictionary: [String:Any] = [:]){
      self.hadoopBuildVersion = dictionary["hadoopBuildVersion"] as? String ?? ""
      self.hadoopVersion = dictionary["hadoopVersion"] as? String ?? ""
      self.hadoopVersionBuiltOn = dictionary["hadoopVersionBuiltOn"] as? String ?? ""
      self.startedOn = dictionary["startedOn"] as? Int ?? 0
    }//init
  }//Historyinfo

  /// constructor of MapReduceHistroy
  /// - parameters:
  ///   - service: the protocol of web request - http / https
  ///		- host: the hostname or ip address of the MapReduce History Server
  ///		- port: the port of MapReduce History Server
  ///   - user: the MapReduce User
  ///		- auth: Authorization Model. Please check the enum Authentication for details
  ///   - proxyUser: proxy user, if applicable
  ///		- apibase: use this parameter ONLY the target server has a different api routine other than /ws/v1/history
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
  ///   HistoryInfo structure, See HistoryInfo.
  /// - throws:
  ///   Exceptions
  @discardableResult
  public func checkInfo() throws -> Info? {
    let url = assembleURL("")
    let (_, dat, _) = try self.perform(overwriteURL: url)
    let dic = try dat.jsonDecode() as? [String:Any] ?? [:]
    return Info(dic["historyInfo"] as? [String:Any] ?? [:])
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
  ///   Exceptions
  /// - returns:
  ///   [Job], an array of Job Structures
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
    let dic = try dat.jsonDecode() as? [String:Any] ?? [:]
    let jobs = dic["jobs"] as? [String: Any] ?? [:]
    return (jobs["job"] as? [Any] ?? []).map {Job($0 as? [String:Any] ?? [:])}
  }//end func

  ///The jobs resource provides a list of the jobs running on this application master. See also Job API for syntax of the job object.
  /// - returns:
  ///   Job?: a Job Structure.
  /// - throws:
  ///   Exception
  /// - parameters:
  ///   jobId: String, the id of a specific job
  @discardableResult
  public func checkJob(jobId: String) throws -> Job? {
    guard !jobId.isEmpty else {
      throw Exception.insufficientParameters
    }//end guard
    let (_, dat, _) = try perform(overwriteURL: assembleURL("/mapreduce/jobs/\(jobId)"))
    let dic = try dat.jsonDecode() as? [String:Any] ?? [:]
    return Job(dic["job"] as? [String:Any] ?? [:])
  }//end func

  /// With the job attempts API, you can obtain a collection of resources that represent a job attempt. When you run a GET operation on this resource, you obtain a collection of Job Attempt Objects.
  /// - parameters:
  ///   - jobId: the job's id to check
  /// - throws
  ///   Exceptions
  /// - returns:
  ///   [JobAttempt], an array of JobAttemp structures
  @discardableResult
  public func checkJobAttempts(jobId: String) throws -> [JobAttempt] {
    guard !jobId.isEmpty else {
      throw Exception.insufficientParameters
    }//end guard
    let url = assembleURL("/mapreduce/jobs/\(jobId)/jobattempts")
    let (_, dat, _) = try self.perform(overwriteURL: url)
    let dic = try dat.jsonDecode() as? [String:Any] ?? [:]
    let jobs = dic["jobAttempts"] as? [String: Any] ?? [:]
    return (jobs["jobAttempt"] as? [Any] ?? []).map {JobAttempt($0 as? [String:Any] ?? [:])}
  }//end func

  /// With the job counters API, you can object a collection of resources that represent al the counters for that job.
  /// - parameters:
  ///   - jobId: the job's id to check
  /// - throws
  ///   Exceptions
  /// - returns:
  ///   JobCounters, see JobCounters structure
  @discardableResult
  public func checkJobCounters(jobId: String) throws -> JobCounters? {
    guard !jobId.isEmpty else {
      throw Exception.insufficientParameters
    }//end guard
    let url = assembleURL("/mapreduce/jobs/\(jobId)/counters")
    let (_, dat, _) = try self.perform(overwriteURL: url)
    let dic = try dat.jsonDecode() as? [String:Any] ?? [:]
    return JobCounters(dic["jobCounters"] as? [String:Any] ?? [:])
  }//end func

  /// A job configuration resource contains information about the job configuration for this job.
  /// - parameters:
  ///   - jobId: the job's id to check
  /// - throws
  ///   Exceptions
  /// - returns:
  ///   JobConfig, see JobConfig structure
  @discardableResult
  public func checkJobConfig(jobId: String) throws -> JobConfig? {
    guard !jobId.isEmpty else {
      throw Exception.insufficientParameters
    }//end guard
    let url = assembleURL("/mapreduce/jobs/\(jobId)/conf")
    let (_, dat, _) = try self.perform(overwriteURL: url)
    let dic = try dat.jsonDecode() as? [String:Any] ?? [:]
    return JobConfig(dic["conf"] as? [String:Any] ?? [:])
  }//end func

  public enum QueryTaskType: String {
  case MAP = "m", REDUCE = "r"
  }//end QueryTaskType

  /// A Task resource contains information about a particular task within a job.
  /// - parameters:
  ///   - jobId: the job's id to check
  /// - throws
  ///   Exceptions
  /// - returns:
  ///   [JobTask], an array of JobTask structures
  @discardableResult
  public func checkJobTasks(jobId: String, taskType: QueryTaskType? = nil) throws -> [JobTask] {
    guard !jobId.isEmpty else {
      throw Exception.insufficientParameters
    }//end guard
    var url = assembleURL("/mapreduce/jobs/\(jobId)/tasks")
    if taskType != nil {
      url.append("?type=" + (taskType?.rawValue ?? ""))
    }
    let (_, dat, _) = try self.perform(overwriteURL: url)
    let dic = try dat.jsonDecode() as? [String:Any] ?? [:]
    let tasks = dic["tasks"] as? [String: Any] ?? [:]
    return (tasks["task"] as? [Any] ?? []).map {JobTask($0 as? [String:Any] ?? [:])}
  }//end checkJobTasks
  
  /// A Task resource contains information about a particular task within a job.
  /// - parameters:
  ///   - jobId: the job's id to check
  ///   - taskId: the task id of a job
  /// - throws
  ///   Exceptions
  /// - returns:
  ///   JobTask, see JobTask structure
  @discardableResult
  public func checkJobTask(jobId: String, taskId: String) throws -> JobTask? {
    guard !jobId.isEmpty else {
      throw Exception.insufficientParameters
    }//end guard
    let url = assembleURL("/mapreduce/jobs/\(jobId)/tasks/\(taskId)")
    let (_, dat, _) = try self.perform(overwriteURL: url)
    let dic = try dat.jsonDecode() as? [String:Any] ?? [:]
    return JobTask(dic["task"] as? [String: Any] ?? [:])
  }//end checkJobTasks

  /// With the task counters API, you can object a collection of resources that represent all the counters for that task.
  /// - parameters:
  ///   - jobId: the job's id to check
  ///   - taskId: the task id of a job
  /// - throws
  ///   Exceptions
  /// - returns:
  ///   JobTaskCounters, see JobTaskCounters structure
  @discardableResult
  public func checkJobTaskCounters(jobId: String, taskId: String) throws -> JobTaskCounters? {
    guard !jobId.isEmpty else {
      throw Exception.insufficientParameters
    }//end guard
    let url = assembleURL("/mapreduce/jobs/\(jobId)/tasks/\(taskId)/counters")
    let (_, dat, _) = try self.perform(overwriteURL: url)
    let dic = try dat.jsonDecode() as? [String:Any] ?? [:]
    return JobTaskCounters(dic["jobTaskCounters"] as? [String:Any] ?? [:])
  }//end checkJobTaskCounters

  /// With the task attempts API, you can obtain a collection of resources that represent a task attempt within a job. When you run a GET operation on this resource, you obtain a collection of Task Attempt Objects.
  /// - parameters:
  ///   - jobId: the job's id to check
  ///   - taskId: the task id of a job
  /// - throws
  ///   Exceptions
  /// - returns:
  ///   [TaskAttempt], see TaskAttempt data structure
  @discardableResult
  public func checkJobTaskAttempts(jobId: String, taskId: String) throws -> [TaskAttempt] {
    guard !jobId.isEmpty else {
      throw Exception.insufficientParameters
    }//end guard
    let url = assembleURL("/mapreduce/jobs/\(jobId)/tasks/\(taskId)/attempts")
    let (_, dat, _) = try self.perform(overwriteURL: url)
    return dat.asTaskAttemps
  }//end checkJobTaskAttempts

  /// A Task Attempt resource contains information about a particular task attempt within a job.
  /// - parameters:
  ///   - jobId: the job's id to check
  ///   - taskId: the task id of a job
  ///   - attemptId: id of the task attempt
  /// - throws
  ///   Exceptions
  /// - returns:
  ///   TaskAttempt?, see TaskAttempt data structure
  @discardableResult
  public func checkJobTaskAttempt(jobId: String, taskId: String, attemptId: String) throws -> TaskAttempt? {
    guard !jobId.isEmpty else {
      throw Exception.insufficientParameters
    }//end guard
    let url = assembleURL("/mapreduce/jobs/\(jobId)/tasks/\(taskId)/attempts/\(attemptId)")
    let (_, dat, _) = try self.perform(overwriteURL: url)
    return dat.asTaskAttemp
  }//end checkJobTaskAttempt

  /// With the task attempt counters API, you can object a collection of resources that represent al the counters for that task attempt.
  /// - parameters:
  ///   - jobId: the job's id to check
  ///   - taskId: the task id of a job
  ///   - attemptId: id of the task attempt
  /// - throws
  ///   Exceptions
  /// - returns:
  ///   JobTaskAttemptCounters?, see JobTaskAttemptCounters data structure
  @discardableResult
  public func checkJobTaskAttemptCounters(jobId: String, taskId: String, attemptId: String) throws -> JobTaskAttemptCounters? {
    guard !jobId.isEmpty else {
      throw Exception.insufficientParameters
    }//end guard
    let url = assembleURL("/mapreduce/jobs/\(jobId)/tasks/\(taskId)/attempts/\(attemptId)/counters")
    let (_, dat, _) = try self.perform(overwriteURL: url)
    let dic = try dat.jsonDecode() as? [String:Any] ?? [:]
    return JobTaskAttemptCounters(dic["jobTaskAttemptCounters"] as? [String:Any] ?? [:])
  }//end checkJobTasks
}//end MapReduceHistory
