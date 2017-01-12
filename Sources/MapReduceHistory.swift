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

/// With the job counters API, you can object a collection of resources that represent al the counters for that job.
public struct JobCounters{
  public struct CounterGroup{
    public struct Counter{
      /// The counter value of map tasks
      var mapCounterValue = 0
      /// The name of the counter
      var name = ""
      /// The counter value of reduce tasks
      var reduceCounterValue = 0
      /// The counter value of all tasks
      var totalCounterValue = 0
      /// constructor of Counter
      /// - parameters:
      /// a dictionary decoded from json string
      public init(_ dictionary: [String:Any] = [:]) {
        self.mapCounterValue = dictionary["mapCounterValue"] as? Int ?? 0
        self.name = dictionary["name"] as? String ?? ""
        self.reduceCounterValue = dictionary["reduceCounterValue"] as? Int ?? 0
        self.totalCounterValue = dictionary["totalCounterValue"] as? Int ?? 0
      }//init
    }//Counter
    /// A collection of counter objects
    var counters : [Counter] = []
    /// The name of the counter group
    var counterGroupName = ""
    /// constructor of CounterGroup
    /// - parameters:
    /// a dictionary decoded from json string
    public init(_ dictionary: [String:Any] = [:]) {
      self.counters = (dictionary["counter"] as? [Any] ?? []).map{Counter($0 as? [String : Any] ?? [:])}
      self.counterGroupName = dictionary["counterGroupName"] as? String ?? ""
    }//init
  }//CounterGroup
  /// A collection of counter group objects
  var counterGroup : [CounterGroup] = []
  /// The job id
  var id = ""
  /// constructor of JobCounters
  /// - parameters:
  /// a dictionary decoded from json string
  public init(_ dictionary: [String:Any] = [:]) {
    self.counterGroup = (dictionary["counterGroup"] as? [Any] ?? []).map{CounterGroup($0 as? [String : Any] ?? [:])}
    self.id = dictionary["id"] as? String ?? ""
  }//init
}//Jobcounters

extension String {
  /// nicely convert a json string into a JobCounters structure
  public var asJobcounters: JobCounters? {
    get{
      do{
        let dic = try self.jsonDecode() as? [String:Any] ?? [:]
        return JobCounters(dic["jobCounters"] as? [String:Any] ?? [:])
      }catch{
        return nil
      }//end do
    }//end get
  }//end member
}//end extension

/// A job configuration resource contains information about the job configuration for this job.
public struct JobConfig{
  /// Elements of the property object
  public struct Property{
    /// The value of the configuration property
    var value = ""
    /// The name of the configuration property
    var name = ""
    /// The location this configuration object came from. If there is more then one of these it shows the history with the latest source at the end of the list.
    var source = [String]()
    /// constructor of Property
    /// - parameters:
    /// a dictionary decoded from json string
    public init(_ dictionary: [String:Any] = [:]) {
      self.value = dictionary["value"] as? String ?? ""
      self.name = dictionary["name"] as? String ?? ""
      self.source = dictionary["source"] as? [String] ?? []
    }//init
  }//Property
  /// The path to the job configuration file
  var path = ""
  /// Collection of configuration property objects
  var property = [Property]()
  /// constructor of CounterGroup
  /// - parameters:
  /// a dictionary decoded from json string
  public init(_ dictionary: [String:Any] = [:]) {
    self.property = (dictionary["property"] as? [Any] ?? []).map{Property($0 as? [String : Any] ?? [:])}
    self.path = dictionary["path"] as? String ?? ""
  }//init
}//JobConfig

extension String {
  /// nicely convert a json string into a JobCounters structure
  public var asJobConfig: JobConfig? {
    get{
      do{
        let dic = try self.jsonDecode() as? [String:Any] ?? [:]
        return JobConfig(dic["conf"] as? [String:Any] ?? [:])
      }catch{
        return nil
      }//end do
    }//end get
  }//end member
}//end extension

/// A Task resource contains information about a particular task within a job.
public struct JobTask{
  /// The state of the task - valid values are: NEW, SCHEDULED, RUNNING, SUCCEEDED, FAILED, KILL_WAIT, KILLED
  public enum State: String {
    case NEW = "NEW", SCHEDULED = "SCHEDULED", RUNNING = "RUNNING", SUCCEEDED = "SUCCEEDED", FAILED = "FAILED", KILL_WAIT = "KILL_WAIT", KILLED = "KILLED", INVALID = ""
  }//end enum

  /// The task type - MAP or REDUCE
  public enum TaskType: String {
    case MAP = "MAP", REDUCE = "REDUCE", INVALID = ""
  }//end enum

  /// The elapsed time since the application started (in ms)
  var elapsedTime = 0
  /// The time in which the task finished (in ms since epoch)
  var finishTime = 0
  /// The task id
  var id = ""
  /// The progress of the task as a percent
  var progress: Double = 0
  /// The time in which the task started (in ms since epoch) or -1 if it was never started
  var startTime = -1
  /// The state of the task - valid values are: NEW, SCHEDULED, RUNNING, SUCCEEDED, FAILED, KILL_WAIT, KILLED
  var state:State = .INVALID
  /// The id of the last successful attempt
  var successfulAttempt = ""
  /// The task type - MAP or REDUCE
  var type: TaskType = .INVALID

  /// constructor of JobTask
  /// - parameters:
  /// a dictionary decoded from a json string
  public init(_ dictionary: [String:Any] = [:]) {
    self.elapsedTime = dictionary["elapsedTime"] as? Int ?? 0
    self.finishTime = dictionary["finishTime"] as? Int ?? 0
    self.id = dictionary["id"] as? String ?? ""
    self.progress = Double(dictionary["progress"] as? String ?? "") ?? 0.0
    self.startTime = dictionary["startTime"] as? Int ?? 0
    self.state = State(rawValue: dictionary["state"] as? String ?? "") ?? .INVALID
    self.successfulAttempt = dictionary["successfulAttempt"] as? String ?? ""
    self.type = TaskType(rawValue: dictionary["type"] as? String ?? "") ?? .INVALID
  }//init
}//JobTask

extension String {
  public var asJobTasks: [JobTask] {
    get {
      do {
        let dic = try self.jsonDecode() as? [String:Any] ?? [:]
        let tasks = dic["tasks"] as? [String: Any] ?? [:]
        return (tasks["task"] as? [Any] ?? []).map {JobTask($0 as? [String:Any] ?? [:])}
      }catch {
        return []
      }//end do
    }//end get
  }//end member
  public var asJobTask: JobTask? {
    get {
      do {
        let dic = try self.jsonDecode() as? [String:Any] ?? [:]
        return JobTask(dic["task"] as? [String: Any] ?? [:])
      }catch {
        return nil
      }//end do
    }//end get
  }//end member
}//end extension

/// With the task counters API, you can object a collection of resources that represent all the counters for that task.
public struct JobTaskCounters{
  public struct CounterGroup{
    public struct Counter{
      /// The name of the counter
      var name = ""
      /// The value of the counter
      var value = 0
      /// constructor of Counter
      /// - parameters:
      /// a dictionary decoded from json string
      public init(_ dictionary: [String:Any] = [:]) {
        self.name = dictionary["name"] as? String ?? ""
        self.value = dictionary["value"] as? Int ?? 0
      }//init
    }//Counter
    /// A collection of counter objects
    var counters : [Counter] = []
    /// The name of the counter group
    var counterGroupName = ""
    /// constructor of CounterGroup
    /// - parameters:
    /// a dictionary decoded from json string
    public init(_ dictionary: [String:Any] = [:]) {
      self.counters = (dictionary["counter"] as? [Any] ?? []).map{Counter($0 as? [String : Any] ?? [:])}
      self.counterGroupName = dictionary["counterGroupName"] as? String ?? ""
    }//init
  }//CounterGroup
  /// The task id
  var id = ""
  /// A collection of counter group objects
  var taskCounterGroup : [CounterGroup] = []
  /// constructor of JobCounters
  /// - parameters:
  /// a dictionary decoded from json string
  public init(_ dictionary: [String:Any] = [:]) {
    self.id = dictionary["id"] as? String ?? ""
    self.taskCounterGroup = (dictionary["taskCounterGroup"] as? [Any] ?? []).map{CounterGroup($0 as? [String : Any] ?? [:])}
  }//init
}//Jobcounters

extension String {
  /// nicely convert a json string into a JobTaskCounters structure
  public var asJobTaskCounters: JobTaskCounters? {
    get{
      do{
        let dic = try self.jsonDecode() as? [String:Any] ?? [:]
        return JobTaskCounters(dic["jobTaskCounters"] as? [String:Any] ?? [:])
      }catch{
        return nil
      }//end do
    }//end get
  }//end member
}//end extension


/// With the task attempts API, you can obtain a collection of resources that represent a task attempt within a job. When you run a GET operation on this resource, you obtain a collection of Task Attempt Objects.
public struct TaskAttempt {
  /// The state of the task attempt - valid values are: NEW, UNASSIGNED, ASSIGNED, RUNNING, COMMIT_PENDING, SUCCESS_CONTAINER_CLEANUP, SUCCEEDED, FAIL_CONTAINER_CLEANUP, FAIL_TASK_CLEANUP, FAILED, KILL_CONTAINER_CLEANUP, KILL_TASK_CLEANUP, KILLED
  public enum State: String {
    case NEW="NEW",  UNASSIGNED="UNASSIGNED",  ASSIGNED="ASSIGNED",  RUNNING="RUNNING",  COMMIT_PENDING="COMMIT_PENDING",  SUCCESS_CONTAINER_CLEANUP="SUCCESS_CONTAINER_CLEANUP",  SUCCEEDED="SUCCEEDED",  FAIL_CONTAINER_CLEANUP="FAIL_CONTAINER_CLEANUP",  FAIL_TASK_CLEANUP="FAIL_TASK_CLEANUP",  FAILED="FAILED",  KILL_CONTAINER_CLEANUP="KILL_CONTAINER_CLEANUP",  KILL_TASK_CLEANUP="KILL_TASK_CLEANUP",  KILLED = "KILLED", INVALID = ""
  }//end case

  /// The task type - MAP or REDUCE
  public enum TaskType: String {
    case MAP = "MAP", REDUCE = "REDUCE", INVALID = ""
  }//end enum

  /// The task id
  var id = ""

  /// The rack
  var rack = ""

  /// The state of the task attempt - valid values are: NEW, UNASSIGNED, ASSIGNED, RUNNING, COMMIT_PENDING, SUCCESS_CONTAINER_CLEANUP, SUCCEEDED, FAIL_CONTAINER_CLEANUP, FAIL_TASK_CLEANUP, FAILED, KILL_CONTAINER_CLEANUP, KILL_TASK_CLEANUP, KILLED
  var state : State = .INVALID

  /// The type of task, map or reduce
  var type : TaskType = .INVALID

  /// The container id this attempt is assigned to
  var assignedContainerId = ""

  /// The http address of the node this task attempt ran on
  var nodeHttpAddress = ""

  /// A diagnostics message
  var diagnostics = ""

  /// The progress of the task attempt as a percent
  var progress: Double = 0

  /// The time in which the task attempt started (in ms since epoch)
  var startTime = 0

  /// The time in which the task attempt finished (in ms since epoch)
  var finishTime = 0

  /// 	The elapsed time since the task attempt started (in ms)
  var elapsedTime = 0

  /// For reduce task attempts only:
  /// The time at which shuffle finished (in ms since epoch)
  var shuffleFinishTime = 0

  /// For reduce task attempts only:
  /// The time at which merge finished (in ms since epoch)
  var mergeFinishTime = 0

  /// For reduce task attempts only:
  /// The time it took for the shuffle phase to complete (time in ms between reduce task start and shuffle finish)
  var elapsedShuffleTime = 0

  /// For reduce task attempts only:
  /// The time it took for the merge phase to complete (time in ms between the shuffle finish and merge finish)
  var elapsedMergeTime = 0

  /// For reduce task attempts only:
  /// 	The time it took for the reduce phase to complete (time in ms between merge finish to end of reduce task)
  var elapsedReduceTime = 0

  /// constructor of TaskAttempt
  ///
  /// - parameters:
  /// a dictionary decoded from a json string
  public init(_ dictionary: [String:Any] = [:]) {

    self.id = dictionary["id"] as? String ?? ""

    self.state = State(rawValue : dictionary["state"] as? String ?? "") ?? .INVALID

    self.rack = dictionary["rack"] as? String ?? ""

    self.type = TaskType(rawValue : dictionary["type"] as? String ?? "" ) ?? .INVALID

    self.assignedContainerId = dictionary["assignedContainerId"] as? String ?? ""

    self.nodeHttpAddress = dictionary["nodeHttpAddress"] as? String ?? ""

    self.diagnostics = dictionary["diagnostics"] as? String ?? ""

    self.progress = Double(dictionary["progress"] as? String ?? "0.0") ?? 0.0

    self.startTime = dictionary["startTime"] as? Int ?? 0

    self.finishTime = dictionary["finishTime"] as? Int ?? 0

    self.elapsedTime = dictionary["elapsedTime"] as? Int ?? 0

    /// For reduce task attempts only:
    self.shuffleFinishTime = dictionary["shuffleFinishTime"] as? Int ?? 0
    self.mergeFinishTime = dictionary["mergeFinishTime"] as? Int ?? 0
    self.elapsedShuffleTime = dictionary["elapsedShuffleTime"] as? Int ?? 0
    self.elapsedMergeTime = dictionary["elapsedMergeTime"] as? Int ?? 0
    self.elapsedReduceTime = dictionary["elapsedReduceTime"] as? Int ?? 0

  }//init
}//task

extension String {
  /// quick cast to TaskAttempt
  public var asTaskAttemp: TaskAttempt? {
    get {
      do {
        let dic = try self.jsonDecode() as? [String:Any] ?? [:]
        return TaskAttempt(dic["taskAttempt"] as? [String: Any] ?? [:])
      }catch {
        return nil
      }//end do
    }//end get
  }//end member
  /// quick cast to [TaskAttempt]
  public var asTaskAttemps: [TaskAttempt] {
    get {
      do {
        let dic = try self.jsonDecode() as? [String:Any] ?? [:]
        let t = dic["taskAttempts"] as? [String: Any] ?? [:]
        return (t["taskAttempt"] as? [Any] ?? []).map {TaskAttempt($0 as? [String:Any] ?? [:])}
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

  /// With the job counters API, you can object a collection of resources that represent al the counters for that job.
  /// - parameters:
  ///   - jobId: the job's id to check
  /// - throws
  /// WebHDFS.Exceptions
  /// - returns:
  /// JobCounters, see JobCounters structure
  @discardableResult
  public func checkJobCounters(jobId: String) throws -> JobCounters? {
    guard !jobId.isEmpty else {
      throw Exception.insufficientParameters
    }//end guard
    let url = assembleURL("/mapreduce/jobs/\(jobId)/counters")
    let (_, dat, _) = try self.perform(overwriteURL: url)
    return dat.asJobcounters
  }//end func

  /// A job configuration resource contains information about the job configuration for this job.
  /// - parameters:
  ///   - jobId: the job's id to check
  /// - throws
  /// WebHDFS.Exceptions
  /// - returns:
  /// JobConfig, see JobConfig structure
  @discardableResult
  public func checkJobConfig(jobId: String) throws -> JobConfig? {
    guard !jobId.isEmpty else {
      throw Exception.insufficientParameters
    }//end guard
    let url = assembleURL("/mapreduce/jobs/\(jobId)/conf")
    let (_, dat, _) = try self.perform(overwriteURL: url)
    return dat.asJobConfig
  }//end func

  public enum QueryTaskType: String {
  case MAP = "m", REDUCE = "r"
  }//end QueryTaskType

  /// A Task resource contains information about a particular task within a job.
  /// - parameters:
  ///   - jobId: the job's id to check
  /// - throws
  /// WebHDFS.Exceptions
  /// - returns:
  /// [JobTask], an array of JobTask structures
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
    return dat.asJobTasks
  }//end checkJobTasks

  /// A Task resource contains information about a particular task within a job.
  /// - parameters:
  ///   - jobId: the job's id to check
  ///   - taskId: the task id of a job
  /// - throws
  /// WebHDFS.Exceptions
  /// - returns:
  /// JobTask, see JobTask structure
  @discardableResult
  public func checkJobTask(jobId: String, taskId: String) throws -> JobTask? {
    guard !jobId.isEmpty else {
      throw Exception.insufficientParameters
    }//end guard
    let url = assembleURL("/mapreduce/jobs/\(jobId)/tasks/\(taskId)")
    let (_, dat, _) = try self.perform(overwriteURL: url)
    return dat.asJobTask
  }//end checkJobTasks

  /// With the task counters API, you can object a collection of resources that represent all the counters for that task.
  /// - parameters:
  ///   - jobId: the job's id to check
  ///   - taskId: the task id of a job
  /// - throws
  /// WebHDFS.Exceptions
  /// - returns:
  /// JobTaskCounters, see JobTaskCounters structure
  @discardableResult
  public func checkJobTaskCounters(jobId: String, taskId: String) throws -> JobTaskCounters? {
    guard !jobId.isEmpty else {
      throw Exception.insufficientParameters
    }//end guard
    let url = assembleURL("/mapreduce/jobs/\(jobId)/tasks/\(taskId)/counters")
    let (_, dat, _) = try self.perform(overwriteURL: url)
    return dat.asJobTaskCounters
  }//end checkJobTasks

  /// With the task attempts API, you can obtain a collection of resources that represent a task attempt within a job. When you run a GET operation on this resource, you obtain a collection of Task Attempt Objects.
  /// - parameters:
  ///   - jobId: the job's id to check
  ///   - taskId: the task id of a job
  /// - throws
  /// WebHDFS.Exceptions
  /// - returns:
  /// [TaskAttempt], see TaskAttempt data structure
  @discardableResult
  public func checkJobTaskAttempts(jobId: String, taskId: String) throws -> [TaskAttempt] {
    guard !jobId.isEmpty else {
      throw Exception.insufficientParameters
    }//end guard
    let url = assembleURL("/mapreduce/jobs/\(jobId)/tasks/\(taskId)/attempts")
    let (_, dat, _) = try self.perform(overwriteURL: url)
    return dat.asTaskAttemps
  }//end checkJobTasks

  /// A Task Attempt resource contains information about a particular task attempt within a job.
  /// - parameters:
  ///   - jobId: the job's id to check
  ///   - taskId: the task id of a job
  ///   - attemptId: id of the task attempt
  /// - throws
  /// WebHDFS.Exceptions
  /// - returns:
  /// TaskAttempt?, see TaskAttempt data structure
  @discardableResult
  public func checkJobTaskAttempt(jobId: String, taskId: String, attemptId: String) throws -> TaskAttempt? {
    guard !jobId.isEmpty else {
      throw Exception.insufficientParameters
    }//end guard
    let url = assembleURL("/mapreduce/jobs/\(jobId)/tasks/\(taskId)/attempts/\(attemptId)")
    let (_, dat, _) = try self.perform(overwriteURL: url)
    return dat.asTaskAttemp
  }//end checkJobTasks
}//end MapReduceHistory
