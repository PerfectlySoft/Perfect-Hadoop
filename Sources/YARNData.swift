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

