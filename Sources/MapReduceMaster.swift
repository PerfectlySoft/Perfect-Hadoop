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
public class MapReduceApplication: YARNResourceManager {

  /// When you make a request for the mapreduce application master information, the information will be returned as an info object.
  public struct Info{
    /// The application id
    var appId = ""

    /// The time the application started (in ms since epoch)
    var startedOn = 0

    /// The name of the application
    var name = ""

    /// The user name of the user who started the application
    var user = ""

    /// The time since the application was started (in ms)
    var elapsedTime = 0

    /// constructor
    public init(_ dictionary: [String:Any] = [:]) {
      self.appId = dictionary["appId"] as? String ?? ""
      self.elapsedTime = dictionary["elapsedTime"] as? Int ?? 0
      self.name = dictionary["name"] as? String ?? ""
      self.startedOn = dictionary["startedOn"] as? Int ?? 0
      self.user = dictionary["user"] as? String ?? ""
    }//init
  }//Info

  /// A job resource contains information about a particular job that was started by this application master. Certain fields are only accessible if user has permissions - depends on acl settings.
  public struct Job{

    /// the job state - valid values are: NEW, INITED, RUNNING, SUCCEEDED, FAILED, KILL_WAIT, KILLED, ERROR
    public enum State: String{
    case NEW="NEW", INITED="INITED", RUNNING="RUNNING", SUCCEEDED="SUCCEEDED", FAILED="FAILED", KILL_WAIT="KILL_WAIT", KILLED="KILLED", ERROR = "ERROR", INVALID = ""
    }//end enum

    /// A collection of acls objects
    public struct ACL{

      /// The acl name
      var name = ""

      /// The acl value
      var value = ""

      /// constructor of ACL
      /// - parameters:
      ///   a dictionary decoded from json
      public init(_ dictionary: [String:Any] = [:]) {
        self.name = dictionary["name"] as? String ?? ""
        self.value = dictionary["value"] as? String ?? ""
      }//init
    }//aclsItem

    /// A collection of acls objects
    var acls : [ACL] = []

    /// A diagnostic message
    var diagnostics = ""

    /// The elapsed time since job started (in ms)
    var elapsedTime = 0

    /// The number of failed map attempts
    var failedMapAttempts = 0

    /// The number of failed reduce attempts
    var failedReduceAttempts = 0

    /// The time the job finished (in ms since epoch)
    var finishTime = 0

    /// The job id
    var id = ""

    /// The number of killed map attempts
    var killedMapAttempts = 0

    /// The number of killed reduce attempts
    var killedReduceAttempts = 0
    var mapProgress = 0

    /// The number of completed maps
    var mapsCompleted = 0

    /// The number of maps still to be run
    var mapsPending = 0

    /// The number of running maps
    var mapsRunning = 0

    /// The total number of maps
    var mapsTotal = 0

    /// 	The job name
    var name = ""

    /// The number of new map attempts
    var newMapAttempts = 0

    /// The number of new reduce attempts
    var newReduceAttempts = 0
    var reduceProgress = 0

    /// The number of completed reduces
    var reducesCompleted = 0

    /// The number of reduces still to be run
    var reducesPending = 0

    /// The number of running reduces
    var reducesRunning = 0

    /// The total number of reduces
    var reducesTotal = 0

    /// The number of running map attempts
    var runningMapAttempts = 0

    /// The number of running reduce attempts
    var runningReduceAttempts = 0

    /// The time the job started (in ms since epoch)
    var startTime = 0

    /// the job state - valid values are: NEW, INITED, RUNNING, SUCCEEDED, FAILED, KILL_WAIT, KILLED, ERROR
    var state: State = .INVALID

    /// The number of successful map attempts
    var successfulMapAttempts = 0

    /// The number of successful reduce attempts
    var successfulReduceAttempts = 0

    /// Indicates if the job was an uber job - ran completely in the application master
    var uberized = false

    /// The user name
    var user = ""

    /// constructor of job
    /// - parameters:
    ///   a dictionary decoded from json string
    public init(_ dictionary: [String:Any] = [:]) {
      self.acls = (dictionary["acls"] as? [Any] ?? []).map{ACL($0 as? [String : Any] ?? [:])}
      self.diagnostics = dictionary["diagnostics"] as? String ?? ""
      self.elapsedTime = dictionary["elapsedTime"] as? Int ?? 0
      self.failedMapAttempts = dictionary["failedMapAttempts"] as? Int ?? 0
      self.failedReduceAttempts = dictionary["failedReduceAttempts"] as? Int ?? 0
      self.finishTime = dictionary["finishTime"] as? Int ?? 0
      self.id = dictionary["id"] as? String ?? ""
      self.killedMapAttempts = dictionary["killedMapAttempts"] as? Int ?? 0
      self.killedReduceAttempts = dictionary["killedReduceAttempts"] as? Int ?? 0
      self.mapProgress = dictionary["mapProgress"] as? Int ?? 0
      self.mapsCompleted = dictionary["mapsCompleted"] as? Int ?? 0
      self.mapsPending = dictionary["mapsPending"] as? Int ?? 0
      self.mapsRunning = dictionary["mapsRunning"] as? Int ?? 0
      self.mapsTotal = dictionary["mapsTotal"] as? Int ?? 0
      self.name = dictionary["name"] as? String ?? ""
      self.newMapAttempts = dictionary["newMapAttempts"] as? Int ?? 0
      self.newReduceAttempts = dictionary["newReduceAttempts"] as? Int ?? 0
      self.reduceProgress = dictionary["reduceProgress"] as? Int ?? 0
      self.reducesCompleted = dictionary["reducesCompleted"] as? Int ?? 0
      self.reducesPending = dictionary["reducesPending"] as? Int ?? 0
      self.reducesRunning = dictionary["reducesRunning"] as? Int ?? 0
      self.reducesTotal = dictionary["reducesTotal"] as? Int ?? 0
      self.runningMapAttempts = dictionary["runningMapAttempts"] as? Int ?? 0
      self.runningReduceAttempts = dictionary["runningReduceAttempts"] as? Int ?? 0
      self.startTime = dictionary["startTime"] as? Int ?? 0
      self.state = State(rawValue: dictionary["state"] as? String ?? "") ?? .INVALID
      self.successfulMapAttempts = dictionary["successfulMapAttempts"] as? Int ?? 0
      self.successfulReduceAttempts = dictionary["successfulReduceAttempts"] as? Int ?? 0
      self.uberized = dictionary["uberized"] as? Bool ?? false
      self.user = dictionary["user"] as? String ?? ""
    }//init
  }//Job

  /// the real application id
  internal var id = ""

  /// read only property of application id
  public var appId: String { get { return self.id } }

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
  public init (applicationId: String, service: String = "http",
                        host: String = "localhost", port: Int = 8088, user: String = "",
                        auth: Authentication = .off, proxyUser: String = "",
                        apibase: String = "/proxy", timeout: Int = 0) {
    self.id = applicationId
    super.init(service: service, host: host, port: port, user: user, auth: auth, proxyUser: proxyUser,
               apibase: apibase, timeout: timeout)
  }//end constructor

  /// generate a url by the provided information
  /// - returns:
  ///   the assembled url
  ///	- parameters:
  ///		- path: full path of the url
  @discardableResult
  internal func assembleURL(path: String) -> String {
    // assamble the url path
    let url = "\(service)://\(host):\(port)\(base)/\(id)/ws/v1/mapreduce\(path)"
    if self.user.isEmpty {
      return url
    }//end if
    return url + (url.characters.contains("?") ? "&" : "?") + "user.name=" + user.stringByEncodingURL
  }//end assembleURL

  /// When you make a request for the mapreduce application master information, the information will be returned as an info object.
  /// - returns:
  /// Info - See definition of MapReduce Application Info
  /// - throws:
  /// Exception
  @discardableResult
  public func checkInfo() throws -> Info? {
    let (_, dat, _) = try perform(overwriteURL: assembleURL(path: "/info"))
    let dic = try dat.jsonDecode() as? [String:Any] ?? [:]
    return Info(dic["info"] as? [String:Any] ?? [:])
  }//end checkInfo

  ///The jobs resource provides a list of the jobs running on this application master. See also Job API for syntax of the job object.
  /// - returns:
  ///   [Job]: an array of Job Structures.
  /// - throws:
  ///   Exception
  @discardableResult
  public func checkJobs() throws -> [Job] {
    let (_, dat, _) = try perform(overwriteURL: assembleURL(path: "/jobs"))
    let dic = try dat.jsonDecode() as? [String:Any] ?? [:]
    let jobs = dic["jobs"] as? [String:Any] ?? [:]
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
    let (_, dat, _) = try perform(overwriteURL: assembleURL(path: "/jobs/\(jobId)"))
    let dic = try dat.jsonDecode() as? [String:Any] ?? [:]
    return Job(dic["job"] as? [String:Any] ?? [:])
  }//end func

  /// When you make a request for the list of job attempts, the information will be returned as an array of job attempt objects.
  /// - parameters:
  ///   - jobId: String, the id of a specific job
  /// - throws
  ///   Exceptions
  /// - returns:
  ///   [JobAttempt], an array of JobAttemp structures
  @discardableResult
  public func checkJobAttempts(jobId: String) throws -> [JobAttempt] {
    guard !jobId.isEmpty else {
      throw Exception.insufficientParameters
    }//end guard
    let (_, dat, _) = try perform(overwriteURL: assembleURL(path: "/jobs/\(jobId)/jobattempts"))
    return dat.asJobAttempts
  }//end func

  /// With the job counters API, you can object a collection of resources that represent all the counters for that job.
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
    let (_, dat, _) = try perform(overwriteURL: assembleURL(path: "/jobs/\(jobId)/counters"))
    return dat.asJobcounters
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
    let (_, dat, _) = try perform(overwriteURL: assembleURL(path: "/jobs/\(jobId)/conf"))
    return dat.asJobConfig
  }//end func

  /// TaskType for Query
  public enum QueryTaskType: String {
    case MAP = "m", REDUCE = "r"
  }//end QueryTaskType

  /// A Task resource contains information about a particular task within a job.
  /// - parameters:
  ///   - jobId: the job's id to check
  ///   - taskType: m for map and r for reduce
  /// - throws
  ///   Exceptions
  /// - returns:
  ///   [JobTask], an array of JobTask structures
  @discardableResult
  public func checkJobTasks(jobId: String, taskType: QueryTaskType? = nil) throws -> [JobTask] {
    guard !jobId.isEmpty else {
      throw Exception.insufficientParameters
    }//end guard
    var url = assembleURL("/jobs/\(jobId)/tasks")
    if taskType != nil {
      url.append("?type=" + (taskType?.rawValue ?? ""))
    }
    let (_, dat, _) = try self.perform(overwriteURL: url)
    return dat.asJobTasks
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
    let url = assembleURL("/jobs/\(jobId)/tasks/\(taskId)")
    let (_, dat, _) = try self.perform(overwriteURL: url)
    return dat.asJobTask
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
    let url = assembleURL("/jobs/\(jobId)/tasks/\(taskId)/counters")
    let (_, dat, _) = try self.perform(overwriteURL: url)
    return dat.asJobTaskCounters
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
    let url = assembleURL("/jobs/\(jobId)/tasks/\(taskId)/attempts")
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
    let url = assembleURL("/jobs/\(jobId)/tasks/\(taskId)/attempts/\(attemptId)")
    let (_, dat, _) = try self.perform(overwriteURL: url)
    return dat.asTaskAttemp
  }//end checkJobTaskAttempt

  /// With the task attempt state API, you can query the state of a submitted task attempt as well kill a running task attempt by modifying the state of a running task attempt using a PUT request with the state set to “KILLED”. To perform the PUT operation, authentication has to be setup for the AM web services. In addition, you must be authorized to kill the task attempt. Currently you can only change the state to “KILLED”; an attempt to change the state to any other results in a 400 error response. Examples of the unauthorized and bad request errors are below. When you carry out a successful PUT, the iniital response may be a 202. You can confirm that the app is killed by repeating the PUT request until you get a 200, querying the state using the GET method or querying for task attempt information and checking the state. In the examples below, we repeat the PUT request and get a 200 response.
  /// Please note that in order to kill a task attempt, you must have an authentication filter setup for the HTTP interface. The functionality requires that a username is set in the HttpServletRequest. If no filter is setup, the response will be an “UNAUTHORIZED” response.
  /// This feature is currently in the alpha stage and may change in the future.
  /// - parameters:
  ///   - jobId: the job's id to check
  ///   - taskId: the task id of a job
  ///   - attemptId: id of the task attempt
  /// - throws
  ///   Exceptions
  /// - returns:
  ///   TaskAttempt.State, see TaskAttempt data structure
  @discardableResult
  public func checkJobTaskAttemptState(jobId: String, taskId: String, attemptId: String) throws -> TaskAttempt.State {
    guard !jobId.isEmpty else {
      throw Exception.insufficientParameters
    }//end guard
    let url = assembleURL("/jobs/\(jobId)/tasks/\(taskId)/attempts/\(attemptId)/state")
    let (_, dat, _) = try self.perform(overwriteURL: url)
    let dic = try dat.jsonDecode() as? [String:Any] ?? [:]
    guard let state = TaskAttempt.State(rawValue: dic["state"] as? String ?? "") else {
      throw Exception.unexpectedReturn
    }//end guard
    return state
  }//end checkJobTaskAttempt

  /// With the task attempt state API, you can query the state of a submitted task attempt as well kill a running task attempt by modifying the state of a running task attempt using a PUT request with the state set to “KILLED”. To perform the PUT operation, authentication has to be setup for the AM web services. In addition, you must be authorized to kill the task attempt. Currently you can only change the state to “KILLED”; an attempt to change the state to any other results in a 400 error response. Examples of the unauthorized and bad request errors are below. When you carry out a successful PUT, the iniital response may be a 202. You can confirm that the app is killed by repeating the PUT request until you get a 200, querying the state using the GET method or querying for task attempt information and checking the state. In the examples below, we repeat the PUT request and get a 200 response.
  /// Please note that in order to kill a task attempt, you must have an authentication filter setup for the HTTP interface. The functionality requires that a username is set in the HttpServletRequest. If no filter is setup, the response will be an “UNAUTHORIZED” response.
  /// This feature is currently in the alpha stage and may change in the future.
  /// - parameters:
  ///   - jobId: the job's id to check
  ///   - taskId: the task id of a job
  ///   - attemptId: id of the task attempt
  /// - throws
  ///   Exceptions
  /// - returns:
  ///   TaskAttempt.State, see TaskAttempt data structure
  @discardableResult
  public func killTaskAttempt(jobId: String, taskId: String, attemptId: String) throws {
    guard !jobId.isEmpty else {
      throw Exception.insufficientParameters
    }//end guard
    let url = assembleURL("/jobs/\(jobId)/tasks/\(taskId)/attempts/\(attemptId)/state")
    let _ = try submitRequest(url: url, extensionType: "json", content: "{\"state\":\"KILLED\"}")
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
    let url = assembleURL("/jobs/\(jobId)/tasks/\(taskId)/attempts/\(attemptId)/counters")
    let (_, dat, _) = try self.perform(overwriteURL: url)
    return dat.asJobTaskAttemptCounters
  }//end checkJobTasks
}//end class
