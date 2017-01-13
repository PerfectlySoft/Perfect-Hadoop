//
//  MapReduceData.swift
//  PerfectHadoop
//
//  Created by Rocky Wei on 2017-01-13.
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


/// The jobs resource provides a list of the MapReduce jobs.
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
    /// a dictionary decoded from json
    public init(_ dictionary: [String:Any] = [:]) {
      self.name = dictionary["name"] as? String ?? ""
      self.value = dictionary["value"] as? String ?? ""
    }//init
  }//aclsItem

  /// A collection of acls objects
  var acls : [ACL] = []

  /// The average time of a map task (in ms)
  var avgMapTime = 0

  /// The average time of the merge (in ms)
  var avgMergeTime = 0

  /// The average time of the reduce (in ms)
  var avgReduceTime = 0

  /// The average time of the shuffle (in ms)
  var avgShuffleTime = 0

  /// A diagnostic message
  var diagnostics = ""
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

  var mapsPending = 0

  var mapsRunning = 0

  /// The total number of maps
  var mapsTotal = 0

  /// The job name
  var name = ""

  var newMapAttempts = 0

  var newReduceAttempts = 0

  /// The queue the job was submitted to
  var queue = ""

  var reduceProgress: Double = 0

  /// The number of completed reduces
  var reducesCompleted = 0

  var reducesPending = 0

  var reducesRunning = 0

  /// The total number of reduces
  var reducesTotal = 0

  var runningMapAttempts = 0

  var runningReduceAttempts = 0

  /// The time the job started (in ms since epoch)
  var startTime = 0

  /// the job state
  var state: State = .INVALID

  /// The time the job submitted (in ms since epoch)
  var submitTime = 0

  /// The number of successful map attempts
  var successfulMapAttempts = 0

  /// The number of successful reduce attempts
  var successfulReduceAttempts = 0

  /// Indicates if the job was an uber job - ran completely in the application master
  var uberized = false

  /// The user name
  var user = ""

  /// Constructor

  public init(_ dictionary: [String:Any] = [:]) {
    self.acls = (dictionary["acls"] as? [Any] ?? []).map{ACL($0 as? [String : Any] ?? [:])}
    self.avgMapTime = dictionary["avgMapTime"] as? Int ?? 0
    self.avgMergeTime = dictionary["avgMergeTime"] as? Int ?? 0
    self.avgReduceTime = dictionary["avgReduceTime"] as? Int ?? 0
    self.avgShuffleTime = dictionary["avgShuffleTime"] as? Int ?? 0
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
    self.queue = dictionary["queue"] as? String ?? ""
    self.reduceProgress = dictionary["reduceProgress"] as? Double ?? 0.0
    self.reducesCompleted = dictionary["reducesCompleted"] as? Int ?? 0
    self.reducesPending = dictionary["reducesPending"] as? Int ?? 0
    self.reducesRunning = dictionary["reducesRunning"] as? Int ?? 0
    self.reducesTotal = dictionary["reducesTotal"] as? Int ?? 0
    self.runningMapAttempts = dictionary["runningMapAttempts"] as? Int ?? 0
    self.runningReduceAttempts = dictionary["runningReduceAttempts"] as? Int ?? 0
    self.startTime = dictionary["startTime"] as? Int ?? 0
    self.state = State(rawValue: dictionary["state"] as? String ?? "") ?? .INVALID
    self.submitTime = dictionary["submitTime"] as? Int ?? 0
    self.successfulMapAttempts = dictionary["successfulMapAttempts"] as? Int ?? 0
    self.successfulReduceAttempts = dictionary["successfulReduceAttempts"] as? Int ?? 0
    self.uberized = dictionary["uberized"] as? Bool ?? false
    self.user = dictionary["user"] as? String ?? ""
  }//init
}//Job

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
  ///   A dictionary decoded from a json string
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

/// With the task counters API, you can object a collection of resources that represent all the counters for that task.
public struct JobTaskCounters{

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

/// With the task attempt counters API, you can object a collection of resources that represent al the counters for that task attempt.
public struct JobTaskAttemptCounters{

  /// The task id
  var id = ""

  /// A collection of counter group objects
  var taskAttemptCounterGroup : [CounterGroup] = []

  /// constructor of JobTaskAttemptCounters
  /// - parameters:
  /// a dictionary decoded from json string
  public init(_ dictionary: [String:Any] = [:]) {
    self.id = dictionary["id"] as? String ?? ""
    self.taskAttemptCounterGroup = (dictionary["taskAttemptCounterGroup"] as? [Any] ?? []).map{CounterGroup($0 as? [String : Any] ?? [:])}
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

  /// nicely convert a json string into a JobTaskAttemptCounters structure
  public var asJobTaskAttemptCounters: JobTaskAttemptCounters? {
    get{
      do{
        let dic = try self.jsonDecode() as? [String:Any] ?? [:]
        return JobTaskAttemptCounters(dic["jobTaskAttemptCounters"] as? [String:Any] ?? [:])
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
