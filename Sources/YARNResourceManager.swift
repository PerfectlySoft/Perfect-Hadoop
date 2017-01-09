//
//  YARNResourceManager.swift
//  PerfectHadoop
//
//  Created by Rocky Wei on 2017-01-07.
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

/// The cluster information resource provides overall information about the cluster.
public struct ClusterInfo{
  enum State :String {
    case NOTINITED = "NOTINITED", INITED = "INITED", STARTED = "STARTED" , STOPPED = "STOPPED", INVALID = ""
  }//end state

  enum HAState : String {
    case INITIALIZING = "INITIALIZING", ACTIVE = "ACTIVE", STANDBY = "STANDBY", STOPPED = "STOPPED", INVALID = ""
  }//end HAState

  /// Hadoop common build string with build version, user, and checksum
  var hadoopBuildVersion = ""
  /// Version of hadoop common
  var hadoopVersion = ""
  /// Timestamp when hadoop common was built(in ms since epoch)
  var hadoopVersionBuiltOn = ""
  /// The ResourceManager HA state
  var haState: HAState = .STOPPED
  /// The cluster id
  var id = 0
  /// ResourceManager build string with build version, user, and checksum
  var resourceManagerBuildVersion = ""
  /// Version of the ResourceManager
  var resourceManagerVersion = ""
  /// Timestamp when ResourceManager was built (in ms since epoch)
  var resourceManagerVersionBuiltOn = ""
  /// The time the cluster started (in ms since epoch)
  var startedOn = 0
  /// The ResourceManager state - valid values are: NOTINITED, INITED, STARTED, STOPPED
  var state: State = .STOPPED

  /// constructor of ClusterInfo
  /// - parameters:
  ///   - dictionary: [String:Any], a json decoded dictionary of ClusterInfo
  public init(_ dictionary: [String:Any] = [:]) {
    self.hadoopBuildVersion = dictionary["hadoopBuildVersion"] as? String ?? ""
    self.hadoopVersion = dictionary["hadoopVersion"] as? String ?? ""
    self.hadoopVersionBuiltOn = dictionary["hadoopVersionBuiltOn"] as? String ?? ""
    self.id = dictionary["id"] as? Int ?? 0
    self.resourceManagerBuildVersion = dictionary["resourceManagerBuildVersion"] as? String ?? ""
    self.resourceManagerVersion = dictionary["resourceManagerVersion"] as? String ?? ""
    self.resourceManagerVersionBuiltOn = dictionary["resourceManagerVersionBuiltOn"] as? String ?? ""
    self.startedOn = dictionary["startedOn"] as? Int ?? 0
    self.state = State(rawValue: dictionary["state"] as? String ?? "") ?? .INVALID
    self.haState = HAState(rawValue: dictionary["haState"] as? String ?? "") ?? .INVALID
  }//init
}//Clusterinfo

extension String {
  /// an express way of decoding cluster info from a json string
  public var asClusterInfo: ClusterInfo? {
    get{
      do{
        let dic = try self.jsonDecode() as? [String:Any] ?? [:]
        return ClusterInfo(dic["clusterInfo"] as? [String:Any] ?? [:])
      }catch{
        return nil
      }//end do
    }//end get
  }//end member
}//end extension

/// The cluster metrics resource provides some overall metrics about the cluster. More detailed metrics should be retrieved from the jmx interface.
public struct ClusterMetrics{
  /// The number of active nodes
  var activeNodes = 0
  /// The amount of memory allocated in MB
  var allocatedMB = 0
  /// The number of allocated virtual cores
  var allocatedVirtualCores = 0
  /// The number of applications completed
  var appsCompleted = 0
  /// The number of applications failed
  var appsFailed = 0
  /// The number of applications killed
  var appsKilled = 0
  /// The number of applications pending
  var appsPending = 0
  /// The number of applications running
  var appsRunning = 0
  /// The number of applications submitted
  var appsSubmitted = 0
  /// The amount of memory available in MB
  var availableMB = 0
  /// The number of available virtual cores
  var availableVirtualCores = 0
  /// The number of containers allocated
  var containersAllocated = 0
  /// The number of containers pending
  var containersPending = 0
  /// The number of containers reserved
  var containersReserved = 0
  /// The number of nodes decommissioned
  var decommissionedNodes = 0
  /// The number of lost nodes
  var lostNodes = 0
  /// The number of nodes rebooted
  var rebootedNodes = 0
  /// The amount of memory reserved in MB
  var reservedMB = 0
  /// The number of reserved virtual cores
  var reservedVirtualCores = 0
  /// The amount of total memory in MB
  var totalMB = 0
  /// The total number of nodes
  var totalNodes = 0
  /// The total number of virtual cores
  var totalVirtualCores = 0
  /// The number of unhealthy nodes
  var unhealthyNodes = 0

  /// constructor of ClusterMetrics
  /// - parameters:
  ///   - dictionary: [String:Any], a json decoded dictionary of ClusterMetrics
  public init(_ dictionary: [String:Any] = [:]) {
    self.activeNodes = dictionary["activeNodes"] as? Int ?? 0
    self.allocatedMB = dictionary["allocatedMB"] as? Int ?? 0
    self.allocatedVirtualCores = dictionary["allocatedVirtualCores"] as? Int ?? 0
    self.appsCompleted = dictionary["appsCompleted"] as? Int ?? 0
    self.appsFailed = dictionary["appsFailed"] as? Int ?? 0
    self.appsKilled = dictionary["appsKilled"] as? Int ?? 0
    self.appsPending = dictionary["appsPending"] as? Int ?? 0
    self.appsRunning = dictionary["appsRunning"] as? Int ?? 0
    self.appsSubmitted = dictionary["appsSubmitted"] as? Int ?? 0
    self.availableMB = dictionary["availableMB"] as? Int ?? 0
    self.availableVirtualCores = dictionary["availableVirtualCores"] as? Int ?? 0
    self.containersAllocated = dictionary["containersAllocated"] as? Int ?? 0
    self.containersPending = dictionary["containersPending"] as? Int ?? 0
    self.containersReserved = dictionary["containersReserved"] as? Int ?? 0
    self.decommissionedNodes = dictionary["decommissionedNodes"] as? Int ?? 0
    self.lostNodes = dictionary["lostNodes"] as? Int ?? 0
    self.rebootedNodes = dictionary["rebootedNodes"] as? Int ?? 0
    self.reservedMB = dictionary["reservedMB"] as? Int ?? 0
    self.reservedVirtualCores = dictionary["reservedVirtualCores"] as? Int ?? 0
    self.totalMB = dictionary["totalMB"] as? Int ?? 0
    self.totalNodes = dictionary["totalNodes"] as? Int ?? 0
    self.totalVirtualCores = dictionary["totalVirtualCores"] as? Int ?? 0
    self.unhealthyNodes = dictionary["unhealthyNodes"] as? Int ?? 0
  }//init
}//Clustermetrics

extension String {
  /// an express way of decoding cluster metrics info from a json string
  public var asClusterMetrics: ClusterMetrics? {
    get{
      do{
        let dic = try self.jsonDecode() as? [String:Any] ?? [:]
        return ClusterMetrics(dic["clusterMetrics"] as? [String:Any] ?? [:])
      }catch{
        return nil
      }//end do
    }//end get
  }//end member
}//end extension

/// the resource object for resourcesUsed in user and queues
public struct ResourcesUsed{
  /// The amount of memory used (in MB)
  var memory = 0
  /// The number of virtual cores
  var vCores = 0
  /// constructor of ResourcesUsed
  /// - parameters:
  ///   - dictionary: [String:Any], a json decoded dictionary of ResourcesUsed
  public init(_ dictionary: [String:Any] = [:]) {
    self.memory = dictionary["memory"] as? Int ?? 0
    self.vCores = dictionary["vCores"] as? Int ?? 0
  }//init
}//resourcesUsedType

/// user object containing resources used
public struct User {
  /// The username of the user using the resources
  var username = ""
  /// The amount of resources used by the user in this queue
  var resourcesUsed = ResourcesUsed([:])
  /// The number of active applications for this user in this queue
  var numActiveApplications = 0
  /// The number of pending applications for this user in this queue
  var numPendingApplications = 0
  /// constructor of User
  /// - parameters:
  ///   - dictionary: [String:Any], a json decoded dictionary of user
  public init(_ dictionary: [String:Any] = [:]) {
    self.username = dictionary["username"] as? String ?? ""
    self.resourcesUsed = ResourcesUsed(dictionary["resourcesUsed"] as? [String:Any] ?? [:])
    self.numActiveApplications = dictionary["numActiveApplications"] as? Int ?? 0
    self.numPendingApplications = dictionary["numPendingApplications"] as? Int ?? 0
  }//end init
}//end User

/// Queues that can actually have jobs submitted to them are referred to as leaf queues. These queues have additional data associated with them.
public struct Queue {
  /// Absolute capacity percentage this queue can use of entire cluster
  var absoluteCapacity:Double = 0.0
  /// Absolute maximum capacity percentage this queue can use of the entire cluster
  var absoluteMaxCapacity:Double = 0.0
  /// Absolute used capacity percentage this queue is using of the entire cluster
  var absoluteUsedCapacity:Double = 0.0
  /// Configured queue capacity in percentage relative to its parent queue
  var capacity:Double = 0.0
  /// The maximum number of active applications this queue can have
  var maxActiveApplications = 0
  /// The maximum number of active applications per user this queue can have
  var maxActiveApplicationsPerUser = 0
  /// The maximum number of applications this queue can have
  var maxApplications = 0
  /// The maximum number of applications per user this queue can have
  var maxApplicationsPerUser = 0
  /// Configured maximum queue capacity in percentage relative to its parent queue
  var maxCapacity:Double = 0.0
  /// The number of active applications in this queue
  var numActiveApplications = 0
  /// The number of applications currently in the queue
  var numApplications = 0
  /// The number of containers being used
  var numContainers = 0
  /// The number of pending applications in this queue
  var numPendingApplications = 0
  /// The name of the queue
  var queueName = ""
  /// A collection of sub-queue information. Omitted if the queue has no sub-queues.
  var queues = [Queue]()
  /// The total amount of resources used by this queue
  var resourcesUsed = ResourcesUsed([:])
  /// The state of the queue
  var state = ""
  /// type of the queue - capacitySchedulerLeafQueueInfo
  var type = ""
  /// Used queue capacity in percentage
  var usedCapacity:Double = 0.0
  /// A string describing the current resources used by the queue
  var usedResources = ""
  /// The minimum user limit percent set in the configuration
  var userLimit = 0
  /// The user limit factor set in the configuration
  var userLimitFactor:Double = 0.0
  /// A collection of user objects containing resources used
  var users = [User]()
  /// constructor of Queue
  /// - parameters:
  ///   - dictionary: [String:Any], a json decoded dictionary of Queue
  public init(_ dictionary: [String:Any] = [:]) {
    self.absoluteCapacity = Double(any: dictionary["absoluteCapacity"])
    self.absoluteMaxCapacity = Double(any: dictionary["absoluteMaxCapacity"])
    self.absoluteUsedCapacity = Double(any: dictionary["absoluteUsedCapacity"])
    self.capacity = Double(any: dictionary["capacity"])
    self.maxActiveApplications = dictionary["maxActiveApplications"] as? Int ?? 0
    self.maxActiveApplicationsPerUser = dictionary["maxActiveApplicationsPerUser"] as? Int ?? 0
    self.maxApplications = dictionary["maxApplications"] as? Int ?? 0
    self.maxApplicationsPerUser = dictionary["maxApplicationsPerUser"] as? Int ?? 0
    self.maxCapacity = Double(any: dictionary["maxCapacity"])
    self.numActiveApplications = dictionary["numActiveApplications"] as? Int ?? 0
    self.numApplications = dictionary["numApplications"] as? Int ?? 0
    self.numContainers = dictionary["numContainers"] as? Int ?? 0
    self.numPendingApplications = dictionary["numPendingApplications"] as? Int ?? 0
    self.queueName = dictionary["queueName"] as? String ?? ""
    let q = dictionary["queues"] as? [String:Any] ?? [:]
    self.queues = (q["queue"] as? [Any] ?? []).map {Queue($0 as? [String:Any] ?? [:])}
    self.resourcesUsed = ResourcesUsed(dictionary["resourcesUsed"] as? [String:Any] ?? [:])
    self.state = dictionary["state"] as? String ?? ""
    self.type = dictionary["type"] as? String ?? ""
    self.usedCapacity = Double(any: dictionary["usedCapacity"])
    self.usedResources = dictionary["usedResources"] as? String ?? ""
    self.userLimit = dictionary["userLimit"] as? Int ?? 0
    self.userLimitFactor = Double(any: dictionary["userLimitFactor"])
    self.users = (dictionary["users"] as? [Any] ?? []).map {User($0 as? [String:Any] ?? [:])}
  }//init
}//queue

/// Fair Scheduler API
public struct FairQueue {
  /// The maximum number of applications the queue can have
  var maxApps = 0
  /// The configured minimum resources that are guaranteed to the queue
  var minResources = ResourcesUsed([:])
  /// The configured maximum resources that are allowed to the queue
  var maxResources = ResourcesUsed([:])
  /// The sum of resources allocated to containers within the queue
  var usedResources = ResourcesUsed([:])
  /// The queue’s fair share of resources
  var fairResources = ResourcesUsed([:])
  /// The capacity of the cluster
  var clusterResources = ResourcesUsed([:])
  /// The name of the queue
  var queueName = ""
  /// The name of the scheduling policy used by the queue
  var schedulingPolicy = ""
  /// A collection of sub-queue information. Omitted if the queue has no childQueues.
  var childQueues = [FairQueue]()
  /// type of the queue - fairSchedulerLeafQueueInfo
  var type = ""
  /// The number of active applications in this queue
  var numActiveApps = 0
  /// The number of pending applications in this queue
  var numPendingApps = 0
  /// constructor of FairQueue
  /// - parameters:
  ///   - dictionary: [String:Any], a json decoded dictionary of FairQueue
  public init(_ dictionary: [String:Any] = [:]) {
    self.maxApps = dictionary["maxApps"] as? Int ?? 0
    self.minResources = ResourcesUsed(dictionary["minResources"] as? [String:Any] ?? [:])
    self.maxResources = ResourcesUsed(dictionary["maxResources"] as? [String:Any] ?? [:])
    self.usedResources = ResourcesUsed(dictionary["usedResources"] as? [String:Any] ?? [:])
    self.fairResources = ResourcesUsed(dictionary["fairResources"] as? [String:Any] ?? [:])
    self.clusterResources = ResourcesUsed(dictionary["clusterResources"] as? [String:Any] ?? [:])
    self.queueName = dictionary["queueName"] as? String ?? ""
    self.type = dictionary["type"] as? String ?? ""
    self.numActiveApps = dictionary["numActiveApps"] as? Int ?? 0
    self.numPendingApps = dictionary["numPendingApps"] as? Int ?? 0
    let q = dictionary["childQueues"] as? [String:Any] ?? [:]
    self.childQueues = (q["queue"] as? [Any] ?? []).map {FairQueue($0 as? [String:Any] ?? [:])}
  }//init
}// FairQueue



/// The capacity scheduler supports hierarchical queues. This one request will print information about all the queues and any subqueues they have.
public struct SchedulerInfo {

  /// State of the queue - valid values are: STOPPED, RUNNING
  enum QState: String {
    case STOPPED = "STOPPED", RUNNING = "RUNNING", INVALID = ""
  }//end enum

  /// The available node capacity
  var availNodeCapacity = 0
  /// Configured queue capacity in percentage relative to its parent queue
  var capacity: Double = 0
  var maxCapacity: Double = 0
  /// Configured maximum queue capacity in percentage relative to its parent queue
  var maxQueueMemoryCapacity = 0
  /// Minimum queue memory capacity
  var minQueueMemoryCapacity = 0
  /// The number of containers
  var numContainers = 0
  /// The total number of nodes
  var numNodes = 0
  /// State of the queue - valid values are: STOPPED, RUNNING
  var qstate:QState = .INVALID
  /// Name of the queue
  var queueName = ""
  /// A collection of queue resources
  var queues = [Queue]()
  /// A collection of root queue resources
  var rootQueue = FairQueue([:])
  /// The total node capacity
  var totalNodeCapacity = 0
  /// Scheduler type - capacityScheduler
  var type = ""
  /// Used queue capacity in percentage
  var usedCapacity: Double = 0
  /// The used node capacity
  var usedNodeCapacity = 0

  /// constructor of SchedulerInfo
  /// - parameters:
  ///   - dictionary: [String:Any], a json decoded dictionary of SchedulerInfo
  public init(_ dictionary: [String:Any] = [:]) {
    self.availNodeCapacity = dictionary["availNodeCapacity"] as? Int ?? 0
    self.capacity = Double(any: dictionary["capacity"])
    self.maxCapacity = Double(any: dictionary["maxCapacity"])
    self.maxQueueMemoryCapacity = dictionary["maxQueueMemoryCapacity"] as? Int ?? 0
    self.minQueueMemoryCapacity = dictionary["minQueueMemoryCapacity"] as? Int ?? 0
    self.numContainers = dictionary["numContainers"] as? Int ?? 0
    self.numNodes = dictionary["numNodes"] as? Int ?? 0
    self.qstate = QState(rawValue: dictionary["qstate"] as? String ?? "") ?? .INVALID
    self.queueName = dictionary["queueName"] as? String ?? ""
    let q = dictionary["queues"] as? [String:Any] ?? [:]
    self.queues = (q["queue"] as? [Any] ?? []).map {Queue($0 as? [String:Any] ?? [:])}
    self.rootQueue = FairQueue(dictionary["rootQueue"] as? [String:Any] ?? [:])
    self.type = dictionary["type"] as? String ?? ""
    self.usedCapacity = Double(any: dictionary["usedCapacity"])
    self.usedNodeCapacity = dictionary["usedNodeCapacity"] as? Int ?? 0
  }//init
}//schedulerInfo

extension String {
  /// an express way of decoding scheduler info from a json string
  public var asSchedulerInfo: SchedulerInfo? {
    get{
      do{
        let dic = try self.jsonDecode() as? [String:Any] ?? [:]
        let sch = dic["scheduler"] as? [String:Any] ?? [:]
        return SchedulerInfo(sch["schedulerInfo"] as? [String:Any] ?? [:])
      }catch (let err){
        print(err)
        return nil
      }//end do
    }//end get
  }//end member
}//end extension

/// The ResourceManager allow the user to get information about the cluster - status on the cluster, metrics on the cluster, scheduler information, information about nodes in the cluster, and information about applications on the cluster.
public class YARNResourceManager: YARNNodeManager {

  /// constructor of YARNResourceManager
  /// - parameters:
  ///   - service: the protocol of web request - http / https
  ///		- host: the hostname or ip address of the YARN Resource Manager
  ///		- port: the port of YARN Resource Manager
  ///   - user: the YARN User
  ///		- auth: Authorization Model. Please check the enum Authentication for details
  ///   - proxyUser: proxy user, if applicable
  ///   - extraHeaders: extra headers if special header such as CSRF (Cross-Site Request Forgery Prevention) is applicable
  ///		- apibase: use this parameter ONLY the target server has a different api routine other than /webhdfs/v1
  ///   - timeout: timeout in seconds, zero means never timeout during transfer
  public override init (service: String = "http",
                        host: String = "localhost", port: Int = 8088, user: String = "",
                        auth: Authentication = .off, proxyUser: String = "",
                        extraHeaders: [String] = [],
                        apibase: String = "/ws/v1/cluster", timeout: Int = 0) {
    super.init(service: service, host: host, port: port, auth: auth, proxyUser: proxyUser, extraHeaders: extraHeaders, apibase: apibase, timeout: timeout)
  }//end constructor

  /// The cluster information resource provides overall information about the cluster.
  /// - returns:
  /// ClusterInfo structure, See ClusterInfo.
  /// - throws:
  /// WebHDFS.Exceptions
  public func checkClusterInfo() throws -> ClusterInfo? {
    let (_, dat, _) = try self.perform()
    return dat.asClusterInfo
  }//end func

  /// The cluster metrics resource provides some overall metrics about the cluster. More detailed metrics should be retrieved from the jmx interface
  /// - returns:
  /// ClusterMetrics structure, See ClusterMetrics.
  /// - throws:
  /// WebHDFS.Exceptions
  public func checkClusterMetrics() throws -> ClusterMetrics? {
    let (_, dat, _) = try self.perform(overwriteURL: assembleURL("/metrics"))
    return dat.asClusterMetrics
  }//end func

  /// The capacity scheduler supports hierarchical queues. This one request will print information about all the queues and any subqueues they have. Queues that can actually have jobs submitted to them are referred to as leaf queues. These queues have additional data associated with them.
  /// - returns:
  /// SchedulerInfo structure, See SchedulerInfo.
  /// - throws:
  /// WebHDFS.Exceptions
  public func checkSchedulerInfo() throws -> SchedulerInfo? {
    let (_, dat, _) = try self.perform(overwriteURL: assembleURL("/scheduler"))
    return dat.asSchedulerInfo
  }//end func
}//end YARNResourceManager
