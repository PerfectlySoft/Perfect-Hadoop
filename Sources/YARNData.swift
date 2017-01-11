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
  public static func encode(from: [UInt8], autowrap: Bool = false) -> String {
    // create a pipe line to manage the encoded data
    var pipes:[Int32] = [0,0]
    let res = pipe(&pipes)
    guard res == 0 else { return "" }

    // use openssl to encode in base64 form
    let b64 = BIO_new(BIO_f_base64())
    let bio = BIO_new_fd(pipes[1], BIO_NOCLOSE)
    // set this flag to merge all data into one line
    if !autowrap {
      BIO_set_flags(bio, BIO_FLAGS_BASE64_NO_NL)
    }//end

    BIO_push(b64, bio)

    BIO_write(b64, UnsafePointer(from), Int32(from.count))

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

    // read out all bytes encoded, split into lines if need
    repeat {
      // clear the buffer - MUST DO for string ending with zero char
      buf.removeAll(keepingCapacity: true)
      // read the data from pipe to buffer
      received = read(pipes[0], UnsafeMutablePointer(mutating: buf), line)
      // if something read
      if received > 0 {
        // append the buf to the string
        let str = String(cString: buf)
        longStr += str
        // add new line if need
        if autowrap {
          longStr += "\r\n"
        }//end if
      }//end if
    }while(received >= line)

    // finish reading
    close(pipes[0])

    // trim new line ending if if need
    return autowrap ? longStr : String(longStr.characters.filter { !["\n", "\r", "\t", " "].contains($0)})
  }//end func

  public static func encode(from: String, autowrap: Bool = false) -> String {
    return encode(from: [UInt8](from.utf8), autowrap: autowrap)
  }//end func
}//end class

/// Elements of the POST request body am-black-listing-requests object
public class AmBlackListingRequests:JSONConvertibleObject {
  /// Whether AM Blacklisting is enabled
  var amBlackListingEnabled:Bool? = nil
  /// AM Blacklisting disable failure threshold
  var disableFailureThreshold:Double? = nil

  /// constructor
  /// - parameters:
  /// 	- amBlackListingEnabled: Whether AM Blacklisting is enabled
  /// 	- disableFailureThreshold: AM Blacklisting disable failure threshold
  public init(amBlackListingEnabled: Bool? = nil, disableFailureThreshold: Double? = nil) {
    self.amBlackListingEnabled = amBlackListingEnabled
    self.disableFailureThreshold = disableFailureThreshold
  }//end init
  override public func getJSONValues() -> [String : Any] {
    var v:[String:Any] = [:]
    if amBlackListingEnabled != nil {
      v["am-black-listing-enabled"] = amBlackListingEnabled
    }//end if
    if disableFailureThreshold != nil {
      v["disable-failure-threshold"] = disableFailureThreshold
    }//end if
    return v
  }//end func
}//end class
/// Elements of the POST request body log-aggregation-context object
public class LogAggregationContext: JSONConvertibleObject {
  /// The log files which match the defined include pattern will be uploaded when the applicaiton finishes
  var logIncludePattern:String? = nil
  /// The log files which match the defined exclude pattern will not be uploaded when the applicaiton finishes
  var logExcludePattern:String? = nil
  /// The log files which match the defined include pattern will be aggregated in a rolling fashion
  var rolledLogIncludePattern:String? = nil
  /// The log files which match the defined exclude pattern will not be aggregated in a rolling fashion
  var rolledLogExcludePattern:String? = nil
  /// The policy which will be used by NodeManager to aggregate the logs
  var logAggregationPolicyClassName:String? = nil
  /// The parameters passed to the policy class
  var logAggregationPolicyParameters:String? = nil

  /// constructor
  /// - parameters:
  /// 	- logIncludePattern: The log files which match the defined include pattern will be uploaded when the applicaiton finishes
  /// 	- logExcludePattern: The log files which match the defined exclude pattern will not be uploaded when the applicaiton finishes
  /// 	- rolledLogIncludePattern: The log files which match the defined include pattern will be aggregated in a rolling fashion
  /// 	- rolledLogExcludePattern: The log files which match the defined exclude pattern will not be aggregated in a rolling fashion
  /// 	- logAggregationPolicyClassName: The policy which will be used by NodeManager to aggregate the logs
  /// 	- logAggregationPolicyParameters: The parameters passed to the policy class
  public init(logIncludePattern:String? = nil, logExcludePattern:String? = nil, rolledLogIncludePattern:String? = nil, rolledLogExcludePattern: String? = nil, logAggregationPolicyClassName:String? = nil, logAggregationPolicyParameters:String? = nil) {
    self.logIncludePattern = logIncludePattern
    self.logExcludePattern = logExcludePattern
    self.rolledLogIncludePattern = rolledLogIncludePattern
    self.rolledLogExcludePattern = rolledLogExcludePattern
    self.logAggregationPolicyClassName = logAggregationPolicyClassName
    self.logAggregationPolicyParameters = logAggregationPolicyParameters
  }//end init

  override public func getJSONValues() -> [String : Any] {
    var v:[String:Any] = [:]
    if logIncludePattern != nil {
      v["log-include-pattern"] = logIncludePattern
    }//end if
    if logExcludePattern != nil {
      v["log-exclude-pattern"] = logExcludePattern
    }//end if
    if rolledLogIncludePattern != nil {
      v["rolled-log-include-pattern"] = rolledLogIncludePattern
    }//end if
    if rolledLogExcludePattern != nil {
      v["rolled-log-exclude-pattern"] = rolledLogExcludePattern
    }//end if
    if logAggregationPolicyClassName != nil {
      v["log-aggregation-policy-class-name"] = logAggregationPolicyClassName
    }//end if
    if logAggregationPolicyParameters != nil {
      v["log-aggregation-policy-parameters"] = logAggregationPolicyParameters
    }//end if
    return v
  }//end getJSONValue
}//end class

public class ResourceRequest: JSONConvertibleObject {
  //Memory required for each container
  var memory:Int? = nil
  //Virtual cores required for each container
  var vCores:Int? = nil

  /// constructor
  /// - parameters:
  ///   - memory: Memory required for each container
  ///   - vCores: Virtual cores required for each container
  public init(memory:Int? = nil, vCores:Int? = nil) {
    self.memory = memory
    self.vCores = vCores
  }//end init

  override public func getJSONValues() -> [String : Any] {
    var v: [String:Any] = [:]
    if memory != nil {
      v["memory"] = memory
    }//end if
    if vCores != nil {
      v["vCores"] = vCores
    }//end if
    return v
  }//end func
}//end class


/// The credentials object should be used to pass data required for the application to authenticate itself such as delegation-tokens and secrets.
public class Credentials: JSONConvertibleObject {

  /// tokens that you wish to pass to your application, specified as key-value pairs. The key is an identifier for the token and the value is the token(which should be obtained using the respective web-services)
  var tokens: [String:String] = [:]
  /// Secrets that you wish to use in your application, specified as key-value pairs. They key is an identifier and the value is the base-64 encoding of the secret
  var secrets: [String: String] = [:]

  /// constructor
  /// - parameters:
  ///   - tokens:[String: String], tokens that you wish to pass to your application, specified as key-value pairs. The key is an identifier for the token and the value is the token(which should be obtained using the respective web-services)
  ///   - secrets:[String: Any], Secrets that you wish to use in your application, specified as key-value pairs. They key is an identifier and the value will be automatically encoded into base-64.
  public init(tokens:[String: String] = [:], secrets:[String: String] = [:]) {
    self.tokens = tokens
    self.secrets = [:]
    for (k,v) in secrets {
      self.secrets[k] = Base64.encode(from: v)
    }//next
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
}//end Credentials


/// Elements of the local-resources object. The object is a collection of key-value pairs. They key is an identifier for the resources to be localized and the value is the details of the resource.
public class LocalResource: JSONConvertibleObject {
  /// Type of the resource; options are “ARCHIVE”, “FILE”, and “PATTERN”
  public enum ResourceType: String {
    case ARCHIVE = "ARCHIVE", FILE = "FILE", PATTERN = "PATTERN"
  }//end type
  /// options are “PUBLIC”, “PRIVATE”, and “APPLICATION”
  public enum Visibility: String {
    case PUBLIC = "PUBLIC", PRIVATE = "PRIVATE", APPLICATION = "APPLICATION"
  }//end enum

  /// Location of the resource to be localized
  var resource:String? = nil
  /// Type of the resource; options are “ARCHIVE”, “FILE”, and “PATTERN”
  var type: ResourceType? = nil
  /// Visibility the resource to be localized; options are “PUBLIC”, “PRIVATE”, and “APPLICATION”
  var visibility: Visibility? = nil
  /// Size of the resource to be localized
  var size:Int? = nil
  /// Timestamp of the resource to be localized
  var timestamp:Int? = nil

  /// constructor of LocalResource
  /// - parameters:
  ///   - resource: String?, Location of the resource to be localized
  ///   - type: ResourceType?, Type of the resource; options are “ARCHIVE”, “FILE”, and “PATTERN”
  ///   - visibility: Visibility?, Visibility the resource to be localized; options are “PUBLIC”, “PRIVATE”, and “APPLICATION”
  ///   - size: Int?, Size of the resource to be localized
  ///   - timestamp: Int?, Timestamp of the resource to be localized
  public init(resource: String? = nil, type: ResourceType? = nil, visibility: Visibility? = nil, size: Int? = nil, timestamp: Int? = nil) {
    self.resource = resource
    self.type = type
    self.visibility = visibility
    self.size = size
    self.timestamp = timestamp
  }//end init

  override public func getJSONValues() -> [String : Any] {
    var v: [String:Any] = [:]
    if resource != nil {
      v["resource"] = resource
    }//end if

    if type != nil {
      v["type"] = type?.rawValue ?? ""
    }//end if

    if visibility != nil {
      v["visibility"] = visibility?.rawValue ?? ""
    }//end if

    if size != nil {
      v["size"] = size
    }//end if

    if timestamp != nil {
      v["timestamp"] = timestamp
    }//end if
    return v
  }//end func
}//end struct

/// general definition of key-value pairs in Hadoop objects.
public class Entry: JSONConvertibleObject {
  /// for example.  They key is an identifier for the resources to be localized
  public var key: String? = nil
  /// for example. the value is the details of the resource.
  public var value: Any? = nil
  public init(key: String? = nil, value: Any? = nil) {
    self.key = key
    self.value = value
  }//end init
  public override func getJSONValues() -> [String : Any] {
    if key != nil && value != nil {
      return ["key": key ?? "", "value": value ?? JSONConvertibleObject()]
    } else {
      return [:]
    }//end if
  }//end func
}//end class

/// general definition of key-value pairs in Hadoop objects.
public class EncryptedEntry: JSONConvertibleObject {
  /// for example.  They key is an identifier for the resources to be localized
  public var key: String? = nil
  /// for example. the value is the details of the resource.
  public var value: Any? = nil

  /// constructor of B64Entry
  /// - parameters:
  ///   - key: String?, an identifier for the resources to be
  ///   - value: Any?: the details of the resource, will be automatically encoded into a base64 string
  public init(key: String? = nil, value: Any? = nil) {
    self.key = key
    self.value = value
  }//end init

  public override func getJSONValues() -> [String : Any] {
    if key == nil || value == nil {
      return [:]
    }//end if
    if value is String {
      return ["key": key ?? "", "value": Base64.encode(from: value as? String ?? "")]
    }else if value is [UInt8] {
      return ["key": key ?? "", "value": Base64.encode(from: value as? [UInt8] ?? [])]
    }//end if
    return [:]
  }//end func
}//end class

/// Hadoop YARN entries are constructed by an element called 'entry' - CAUTION
public class Entries: JSONConvertibleObject {
  public var entry = [Any]()
  /// - parameters:
  ///   - entry, an array of Entry. See Entry.
  public init(_ entry: [Entry] = []) {
    self.entry = entry
  }//end init
  /// - parameters:
  ///   - entry, an array of EncryptedEntry. See EncryptedEntry.
  public init(_ entry: [EncryptedEntry] = []) {
    self.entry = entry
  }//end init
  public override func getJSONValues() -> [String : Any] {
    if entry.count > 0 {
      return ["entry": entry]
    }else {
      return [:]
    }//end if
  }//end func
}//end class

/// The commands for launching your container, in the order in which they should be executed
/// CAUTION: Hadoop 3.0 alpha document may not be correct about this part
public class Commands: JSONConvertibleObject {
  /// The commands for launching your container, in the order in which they should be executed
  public var command:String? = nil
  /// constructor of commands
  /// - parameters:
  ///   - command: String?, The commands for launching your container, in the order in which they should be executed
  public init(_ command: String? = nil) {
    self.command = command
  }//end public
  public override func getJSONValues() -> [String : Any] {
    if command == nil {
      return [:]
    }else {
      return ["command": command ?? ""]
    }//end if
  }//end func
}//end class


/// The am-container-spec object should be used to provide the container launch context for the application master.
public class AmContainerSpec: JSONConvertibleObject {
  /// Object describing the resources that need to be localized, described as LocalResource
  var localResources: Entries? = nil
  /// Environment variables for your containers, specified as key value pairs
  var environment: Entries? = nil
  /// The commands for launching your container, in the order in which they should be executed
  var commands: Commands? = nil
  /// Application specific service data; key is the name of the auxiliary servce, value is base-64 encoding of the data you wish to pass
  var serviceData:Entries? = nil
  /// The credentials required for your application to run, described as Credentials
  var credentials: Credentials? = nil
  /// ACLs for your application; the key can be “VIEW_APP” or “MODIFY_APP”, the value is the list of users with the permissions
  var applicationAcls: Entries? = nil

  /// constructor of AmContainerSpec, should be used to provide the container launch context for the application master.
  /// - parameters:
  ///   - localResources: Entries?, Object describing the resources that need to be localized, described as LocalResource
  ///   - environment: Entries?, environment variables for your containers, specified as key value pairs
  ///   - commands: Commands?, The commands for launching your container, in the order in which they should be executed
  ///   - serviceData: Entries?, Application specific service data; key is the name of the auxiliary servce, value is base-64 encoding of the data you wish to pass
  ///   - credentials: Credentials?, The credentials required for your application to run, described as Credentials
  ///   - applicationAcls: Entries?, ACLs for your application; the key can be “VIEW_APP” or “MODIFY_APP”, the value is the list of users with the permissions
  public init(localResources: Entries? = nil, environment: Entries? = nil, commands: Commands? = nil, serviceData: Entries? = nil, credentials: Credentials? = nil, applicationAcls: Entries? = nil) {
    self.localResources = localResources
    self.environment = environment
    self.commands = commands
    self.serviceData = serviceData
    self.credentials = credentials
    self.applicationAcls = applicationAcls
  }//end init
  override public func getJSONValues() -> [String : Any] {
    var v: [String: Any] = [:]
    if localResources != nil {
      v["local-resources"] = localResources
    }//end if
    if environment != nil {
      v["environment"] = environment
    }//end if
    if commands != nil {
      v["commands"] = commands
    }//end if
    if serviceData != nil {
      v["service-data"] = serviceData
    }//end if
    if credentials != nil {
      v["credentials"] = credentials
    }//end if
    if applicationAcls != nil {
      v["application-acls"] = applicationAcls
    }//end if
    return v
  }//end func
}//end class

/// The Submit Applications API can be used to submit applications. In case of submitting applications, you must first obtain an application-id using the Cluster NewApplication API. The application-id must be part of the request body. The response contains a URL to the application page which can be used to track the state and progress of your application.
public class SubmitApplication: JSONConvertibleObject {
  /// The application id
  public var id: String? = nil
  /// The application name
  public var name:String? = nil
  /// The name of the queue to which the application should be submitted
  public var queue:String? = nil
  /// The priority of the application
  public var priority:Int? = nil
  /// The application master container launch context, described below
  public var amContainerSpec: AmContainerSpec? = nil
  /// Is the application using an unmanaged application master
  public var unmanagedAM:Bool? = nil
  /// The max number of attempts for this application
  public var maxAppAttempts:Int? = nil
  /// The resources the application master requires, described below
  public var resource: ResourceRequest? = nil
  /// The application type(MapReduce, Pig, Hive, etc)
  public var type:String? = nil
  /// Should YARN keep the containers used by this application instead of destroying them
  public var keepContainersAcrossApplicationAttempts:Bool? = nil
  /// List of application tags
  public var tags = [String]()
  /// Represents all of the information needed by the NodeManager to handle the logs for this application
  public var logAggregationContext: LogAggregationContext? = nil
  /// The failure number will no take attempt failures which happen out of the validityInterval into failure count
  public var attemptFailuresValidityInterval:Int? = nil
  /// Represent the unique id of the corresponding reserved resource allocation in the scheduler
  public var reservationId:String? = nil
  /// Contains blacklisting information such as “enable/disable AM blacklisting” and “disable failure threshold”
  public var amBlackListingRequests: AmBlackListingRequests? = nil

  /// The Submit Applications API can be used to submit applications. In case of submitting applications, you must first obtain an application-id using the Cluster NewApplication API. The application-id must be part of the request body. The response contains a URL to the application page which can be used to track the state and progress of your application.
  /// - parameters:
  ///   - id: String, The application id
  ///   - name: String? The application name
  ///   - queue: String? The name of the queue to which the application should be submitted
  ///   - priority: Int? The priority of the application
  ///   - amContainerSpec: AmContainerSpec? The application master container launch context, described as AmContainerSpec
  ///   - unmanagedAM:Bool? Is the application using an unmanaged application master
  ///   - maxAppAttempts:Int? The max number of attempts for this application
  ///   - resource: ResourceRequest? The resources the application master requires, described as ResourceRequest
  ///   - type:String? The application type(MapReduce, Pig, Hive, etc)
  ///   - keepContainersAcrossApplicationAttempts:Bool? Should YARN keep the containers used by this application instead of destroying them
  ///   - tags: [String], List of application tags
  ///   - logAggregationContext: LogAggregationContext? Represents all of the information needed by the NodeManager to handle the logs for this application
  ///   - attemptFailuresValidityInterval:Int? The failure number will no take attempt failures which happen out of the validityInterval into failure count
  ///   - reservationId:String? Represent the unique id of the corresponding reserved resource allocation in the scheduler
  ///   - amBlackListingRequests: AmBlackListingRequests? Contains blacklisting information such as “enable/disable AM blacklisting” and “disable failure threshold”
  public init(id: String? = nil, name: String? = nil, queue: String? = nil, priority: Int? = nil, amContainerSpec: AmContainerSpec? = nil, unmanagedAM: Bool? = nil, maxAppAttempts: Int? = nil, resource: ResourceRequest? = nil, type: String? = nil, keepContainersAcrossApplicationAttempts: Bool? = nil, tags: [String] = [], logAggregationContext: LogAggregationContext? = nil, attemptFailuresValidityInterval: Int? = nil, reservationId: String? = nil, amBlackListingRequests: AmBlackListingRequests? = nil) {
    self.id = id
    self.name = name
    self.queue = queue
    self.priority = priority
    self.amContainerSpec = amContainerSpec
    self.unmanagedAM = unmanagedAM
    self.maxAppAttempts = maxAppAttempts
    self.resource = resource
    self.type = type
    self.keepContainersAcrossApplicationAttempts = keepContainersAcrossApplicationAttempts
    self.tags = tags
    self.logAggregationContext = logAggregationContext
    self.attemptFailuresValidityInterval = attemptFailuresValidityInterval
    self.reservationId = reservationId
    self.amBlackListingRequests = amBlackListingRequests
  }//end init

  override public func getJSONValues() -> [String : Any] {
    var v:[String: Any] = [:]
    if id != nil {
      v["application-id"]  = id
    }//end if
    if name != nil {
      v["application-name"] = name
    }//end if
    if queue != nil {
      v["queue"] = queue
    }//end if
    if priority != nil {
      v["priority"] = priority
    }//end if
    if amContainerSpec != nil {
      v["am-container-spec"] = amContainerSpec
    }//end if
    if unmanagedAM != nil {
      v["unmanaged-AM"] = unmanagedAM
    }//end if
    if maxAppAttempts != nil {
      v["max-app-attempts"] = maxAppAttempts
    }//end if
    if resource != nil {
      v["resource"] = resource
    }//end if
    if type != nil {
      v["application-type"] = type
    }//end if
    if keepContainersAcrossApplicationAttempts != nil {
      v["keep-containers-across-application-attempts"] = keepContainersAcrossApplicationAttempts
    }//end if
    if tags.count > 0 {
      v["application-tags"] = tags
    }//end if
    if logAggregationContext != nil {
      v["log-aggregation-context"] = logAggregationContext
    }//end if
    if attemptFailuresValidityInterval != nil {
      v["attempt-failures-validity-interval"] = attemptFailuresValidityInterval
    }//end if
    if reservationId != nil {
      v["reservation-id"] = reservationId
    }//end if
    if amBlackListingRequests != nil {
      v["am-black-listing-requests"] = amBlackListingRequests
    }//end if
    return v
  }//end fun
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


