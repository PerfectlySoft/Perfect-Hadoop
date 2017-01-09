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

#if os(Linux)
  import LinuxBridge
#else
  import Darwin
#endif
import COpenSSL
import PerfectLib

/// encode a String by base64 method
/// - returns:
/// base64 encoded text
public class Base64 {
  public static func encode(from: String, autowrap: Bool = false) -> String {
    // create a pipe line to manage the encoded data
    var pipes:[Int32] = [0,0]
    let res = pipe(&pipes)
    guard res == 0 else { return "" }

    // use openssl to encode in base64 form
    let b64 = BIO_new(BIO_f_base64())
    let bio = BIO_new_fd(pipes[1], BIO_NOCLOSE)
    BIO_push(b64, bio)

    from.withCString { ptr in
      let p = unsafeBitCast(ptr, to: UnsafeMutableRawPointer.self)
      BIO_write(b64, p, Int32(from.utf8.count))
    }

    BIO_ctrl(b64,BIO_CTRL_FLUSH,0,nil)
    close(pipes[1])
    BIO_free_all(b64)

    // prepare a 4k buffer to encode
    var buf = [CChar]()
    let size = 4096
    var received = 0
    // encode the file buffer by buffer
    buf.reserveCapacity(size)

    // read encoded data from pipeline and split ascii text
    // into lines of 80 chars per line as defined in RFC 822
    let line = 78

    var longStr = ""
    buf.withUnsafeBufferPointer{ pBuf in
      let pRaw = unsafeBitCast(pBuf.baseAddress, to: UnsafeMutableRawPointer.self)
      repeat {
        memset(pRaw, 0, size)
        received = read(pipes[0], pRaw, line)
        if received > 0 {
          let str = String(cString: buf)
          longStr += str
          if autowrap {
            longStr += "\r\n"
          }
        }//end if
      }while(received >= line)
    }//end buf
    close(pipes[0])
    return longStr
  }//end encode
}//end encode


public class AmBlackListingRequests:JSONConvertibleObject {
  /// Whether AM Blacklisting is enabled
  var amBlackListingEnabled = false
  /// AM Blacklisting disable failure threshold
  var disableFailureThreshold:Double = Double.nan

  /// constructor
  /// - parameters:
  /// See AmBlackListingRequests' member for each
  public init(amBlackListingEnabled: Bool = false, disableFailureThreshold: Double = Double.nan) {
    self.amBlackListingEnabled = amBlackListingEnabled
    self.disableFailureThreshold = disableFailureThreshold
  }//end init
  override public func getJSONValues() -> [String : Any] {
    var v:[String:Any] = [:]
    if amBlackListingEnabled {
      v["am-black-listing-enabled"] = amBlackListingEnabled
    }//end if
    if !disableFailureThreshold.isNaN {
      v["disable-failure-threshold"] = disableFailureThreshold
    }//end if
    return v
  }//end func
}//end class

public class LogAggregationContext: JSONConvertibleObject {
  /// The log files which match the defined include pattern will be uploaded when the applicaiton finishes
  var logIncludePattern = ""
  /// The log files which match the defined exclude pattern will not be uploaded when the applicaiton finishes
  var logExcludePattern = ""
  /// The log files which match the defined include pattern will be aggregated in a rolling fashion
  var rolledLogIncludePattern = ""
  /// The log files which match the defined exclude pattern will not be aggregated in a rolling fashion
  var rolledLogExcludePattern = ""
  /// The policy which will be used by NodeManager to aggregate the logs
  var logAggregationPolicyClassName = ""
  /// The parameters passed to the policy class
  var logAggregationPolicyParameters = ""

  /// constructor
  /// - parameters:
  /// See LogAggregationContext' member for each
  public init(logIncludePattern:String = "", logExcludePattern:String = "", rolledLogIncludePattern:String = "", rolledLogExcludePattern: String = "", logAggregationPolicyClassName:String = "", logAggregationPolicyParameters:String = "") {
    self.logIncludePattern = logIncludePattern
    self.logExcludePattern = logExcludePattern
    self.rolledLogIncludePattern = rolledLogIncludePattern
    self.rolledLogExcludePattern = rolledLogExcludePattern
    self.logAggregationPolicyClassName = logAggregationPolicyClassName
    self.logAggregationPolicyParameters = logAggregationPolicyParameters
  }//end init

  override public func getJSONValues() -> [String : Any] {
    var v:[String:Any] = [:]
    if !logIncludePattern.isEmpty {
      v["log-include-pattern"] = logIncludePattern
    }//end if
    if !logExcludePattern.isEmpty {
      v["log-exclude-pattern"] = logExcludePattern
    }//end if
    if !rolledLogIncludePattern.isEmpty {
      v["rolled-log-include-pattern"] = rolledLogIncludePattern
    }//end if
    if !rolledLogExcludePattern.isEmpty {
      v["rolled-log-exclude-pattern"] = rolledLogExcludePattern
    }//end if
    if !logAggregationPolicyClassName.isEmpty {
      v["log-aggregation-policy-class-name"] = logAggregationPolicyClassName
    }//end if
    if !logAggregationPolicyParameters.isEmpty {
      v["log-aggregation-policy-parameters"] = logAggregationPolicyParameters
    }//end if
    return v
  }//end getJSONValue
}//end class

public class ResourceRequest: JSONConvertibleObject {
  //Memory required for each container
  var memory = 0
  //Virtual cores required for each container
  var vCores = 0

  /// constructor
  /// - parameters:
  ///   - memory:Int, Memory required for each container
  ///   - vCores:Int, Virtual cores required for each container
  public init(memory:Int = 0,vCores:Int = 0) {
    self.memory = memory
    self.vCores = vCores
  }//end init

  override public func getJSONValues() -> [String : Any] {
    var v: [String:Any] = [:]
    if memory > 0 {
      v["memory"] = memory
    }//end if
    if vCores > 0 {
      v["vCores"] = vCores
    }//end if
    return v
  }//end func
}//end class

/// The credentials object should be used to pass data required for the application to authenticate itself such as delegation-tokens and secrets.
public class Credential: JSONConvertibleObject {

  /// tokens that you wish to pass to your application, specified as key-value pairs. The key is an identifier for the token and the value is the token(which should be obtained using the respective web-services)
  var tokens: [String:String] = [:]
  /// Secrets that you wish to use in your application, specified as key-value pairs. They key is an identifier and the value is the base-64 encoding of the secret
  var secrets: [String: String] = [:]

  /// constructor
  /// - parameters:
  ///   - tokens:[String: String], tokens that you wish to pass to your application, specified as key-value pairs. The key is an identifier for the token and the value is the token(which should be obtained using the respective web-services)
  ///   - secrets:[String: String], Secrets that you wish to use in your application, specified as key-value pairs. They key is an identifier and the value is the base-64 encoding of the secret
  public init(tokens:[String: String] = [:], secrets:[String: String] = [:]) {
    self.tokens = tokens
    self.secrets = [:]
    for (k,v) in secrets {
      self.secrets[k] = Base64.encode(from: v)
    }
  }//end init

  override public func getJSONValues() -> [String : Any] {
    var v: [String:Any] = [:]
    if tokens.count > 0 {
      v["tokens"] = tokens
    }//end if
    if secrets.count > 0 {
      v["secrets"] = secrets
    }//end if
    return v
  }//end func
}
/// Elements of the local-resources object. The object is a collection of key-value pairs. They key is an identifier for the resources to be localized and the value is the details of the resource.
public class LocalResource : JSONConvertibleObject {
  /// Type of the resource; options are “ARCHIVE”, “FILE”, and “PATTERN”
  public enum `Type`: String {
    case ARCHIVE = "ARCHIVE", FILE = "FILE", PATTERN = "PATTERN", INVALID = "INVALID"
  }//end type
  /// options are “PUBLIC”, “PRIVATE”, and “APPLICATION”
  public enum Visibility: String {
    case PUBLIC = "PUBLIC", PRIVATE = "PRIVATE", APPLICATION = "APPLICATION", INVALID = "INVALID"
  }//end enum

  var resource = ""
  var type: Type = .INVALID
  var visibility: Visibility = .INVALID
  var size = 0
  var timestamp = 0

  public init(resource: String, type: Type = .INVALID, visibility: Visibility = .INVALID, size: Int = 0, timestamp: Int = 0) {
    self.resource = resource
    self.type = type
    self.visibility = visibility
    self.size = size
    self.timestamp = timestamp
  }//end init

  override public func getJSONValues() -> [String : Any] {
    var v: [String:Any] = [:]
    if !resource.isEmpty {
      v["resource"] = resource
    }//end if

    if type != .INVALID {
      v["type"] = type
    }//end if

    if visibility != .INVALID {
      v["visibility"] = visibility
    }//end if

    if size > 0 {
      v["size"] = size
    }//end if

    if timestamp > 0 {
      v["timestamp"] = timestamp
    }//end if
    return v
  }//end func
}//end class


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

