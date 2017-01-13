//
//  WebHDFSData.swift
//  PerfectHadoop
//
//  Created by Rockford Wei on 2016-01-02.
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

/// Double / Float patch for integer casting
extension Double {
  /// constructor from any numbers and convert it from integer to double
  /// - parameters:
  ///   Any?: any strings or numbers
  public init(any: Any?) {
    switch any {
    case is Int:
      self = Double(any as? Int ?? 0)
    default:
      self = any as? Double ?? 0.0
    }//end case
  }//end init
}//end Double

/// general exceptions
public enum Exception: Error {

  /// errors when perform a specific curl
  case unexpectedResponse (url:String, header: String, body: String)

  /// errors when uploading a file
  case invalidLocalFile (reason: String)

  /// errors when concat a file
  case insufficientParameters

  /// errors when operation not supported by the hdfs system
  case unsupportedOperation (of: String)

  /// unexpectedReturn
  case unexpectedReturn
}//end enum


/// The ACL spec included in ACL modification operations.
public struct AclStatus{

  /// ACL entry
  var entries : [String] = []

  /// the user who is the owner
  var owner = ""

  /// the group owner
  var group = ""

  /// permission string, unix style
  var permission = 0

  /// true if the sticky bit is on
  var stickyBit = false

  /// constructor of AclStatus
  /// - parameters:
  ///   a dictionary decoded from a json string
  public init(_ dictionary: [String:Any] = [:]) {
    self.entries = dictionary["entries"] as? [String] ?? []
    self.owner = dictionary["owner"] as? String ?? ""
    self.group = dictionary["group"] as? String ?? ""
    self.permission = Int(dictionary["permission"] as? String ?? "0") ?? 0
    self.stickyBit = dictionary["stickyBit"] as? Bool ?? false
  }//init
}//AclStatus

extension String {
  public var asAclStatus: AclStatus? {
    get {
      do {
        let dic = try self.jsonDecode() as? [String:Any] ?? [:]
        return AclStatus(dic["AclStatus"] as? [String:Any] ?? [:])
      }catch {
        return nil
      }//end do
    }//end get
  }//end asAclStatus
}//end String

/// XAttrFlag
public enum XAttrFlag:String {
  case CREATE = "CREATE"
  case REPLACE = "REPLACE"
}//end enum

/// The XAttr value of a file/directory.
public struct XAttr{

  /// names are any string prefixed with user./trusted./system./security..
  var name = ""

  /// Enclosed in double quotes or prefixed with 0x or 0s.
  var value = "<>"

  /// constructor of XAttr
  /// - parameters:
  ///   a dictionary decoded from a json string
  public init(_ dictionary: [String:Any] = [:]) {
    self.name = dictionary["name"] as? String ?? ""
    self.value = dictionary["value"] as? String ?? "<>"
  }//init
}//XAttr

extension String {
  public var asXAttrs: [XAttr] {
    get {
      do {
        let dic = try self.jsonDecode() as? [String:Any] ?? [:]
        return (dic["XAttrs"] as? [Any] ?? []).map { XAttr($0 as? [String:Any] ?? [:])}
      } catch {
        return []
      }//end do
    }//end get
  }//end asXAttrs

  /// The XAttr name of a file/directory.
  public var asXAttrNames: [String] {
    get {
      do {
        let dic = try self.jsonDecode() as? [String:Any] ?? [:]
        return dic["XAttrNames"] as? [String] ?? []
      } catch {
        return []
      }//end do
    }//end get
  }// end asXAttrNames

  public var asBoolean: Bool {
    get {
      do {
        let dic = try self.jsonDecode() as? [String:Any] ?? [:]
        return dic["boolean"] as? Bool ?? false
      } catch {
        return false
      }//end do
    }//end get
  }//end asBoolean
}//end String

extension Bool {
  public var string: String {
    get {
      return self ? "true" : "false"
    }//end get
  }//end member
}//end class

public struct ContentSummary{
  public struct Quota{
    // the space consumed
    var consumed = 0
    // quota
    var quota = 0
    public init(_ dictionary: [String:Any] = [:]) {
      self.consumed = dictionary["consumed"] as? Int ?? 0
      self.quota = dictionary["quota"] as? Int ?? 0
    }//init
  }//Quota

  public struct typeQuotaMapping {
    var ARCHIVE = Quota()
    var DISK = Quota()
    var SSD = Quota()
    public init(_ dictionary: [String:Any] = [:]) {
      self.ARCHIVE = Quota(dictionary["ARCHIVE"] as? [String:Any] ?? [:])
      self.DISK = Quota(dictionary["DISK"] as? [String:Any] ?? [:])
      self.SSD = Quota(dictionary["SSD"] as? [String:Any] ?? [:])
    }//end init
  }//end typeQuota

  var directoryCount = 0
  var fileCount = 0
  var length = 0
  var quota = 0
  var spaceConsumed = 0
  var spaceQuota = 0
  var typeQuota = typeQuotaMapping()
  public init(_ dictionary: [String:Any] = [:]) {
    self.directoryCount = dictionary["directoryCount"] as? Int ?? 0
    self.fileCount = dictionary["fileCount"] as? Int ?? 0
    self.length = dictionary["length"] as? Int ?? 0
    self.quota = dictionary["quota"] as? Int ?? 0
    self.spaceConsumed = dictionary["spaceConsumed"] as? Int ?? 0
    self.spaceQuota = dictionary["spaceQuota"] as? Int ?? 0
    self.typeQuota = typeQuotaMapping(dictionary["typeQuota"] as? [String:Any] ?? [:])
  }//init
}//ContentSummary

extension String {
  public var asContentSummary: ContentSummary? {
    get {
      do {
        let dic = try self.jsonDecode() as? [String:Any] ?? [:]
        return ContentSummary(dic["ContentSummary"] as? [String:Any] ?? [:])
      }catch {
        return nil
      }//end do
    }//end get
  }//end AclStatus
}//end asContentSummary

public struct FileChecksum{
  var length = 0
  var algorithm = ""
  var bytes = ""
  public init(_ dictionary: [String:Any] = [:]) {
    self.length = dictionary["length"] as? Int ?? 0
    self.algorithm = dictionary["algorithm"] as? String ?? ""
    self.bytes = dictionary["bytes"] as? String ?? ""
  }//init
}//FileChecksum

extension String {
  public var asFileChecksum: FileChecksum? {
    get {
      do {
        let dic = try self.jsonDecode() as? [String:Any] ?? [:]
        return FileChecksum(dic["FileChecksum"] as? [String:Any] ?? [:])
      }catch {
        return nil
      }//end do
    }//end get
  }//end asFileCheckSum
}//end String

/// common file status parsed from json
public struct FileStatus{

  /// unix time for last access
  var accessTime = 0

  /// file suffix / extension - type
  var pathSuffix = ""

  /// replicated nodes count
  var replication = 0

  /// node type: directory or file
  var type = ""

  /// storage unit, default = 128M, min = 1M
  var blockSize = 0

  /// node owner
  var owner = ""

  /// last modification in unix epoch time format
  var modificationTime = 0

  /// node group info
  var group = ""

  /// node permission, (u)rwx (g)rwx (o)rwx
  var permission = 0
  
  /// file length
  var length = 0
  public init(_ dictionary: [String:Any] = [:]) {
    self.accessTime = dictionary["accessTime"] as? Int ?? 0
    self.pathSuffix = dictionary["pathSuffix"] as? String ?? ""
    self.replication = dictionary["replication"] as? Int ?? 0
    self.type = dictionary["type"] as? String ?? ""
    self.blockSize = dictionary["blockSize"] as? Int ?? 0
    self.owner = dictionary["owner"] as? String ?? ""
    self.modificationTime = dictionary["modificationTime"] as? Int ?? 0
    self.group = dictionary["group"] as? String ?? ""
    self.permission = Int( dictionary["permission"] as? String ?? "0") ?? 0
    self.length = dictionary["length"] as? Int ?? 0
  }//init
}//end FileStatus

extension String {
  public var asFileStatus: FileStatus? {
    get {
      do {
        let dic = try self.jsonDecode() as? [String:Any] ?? [:]
        return FileStatus(dic["FileStatus"] as? [String:Any] ?? [:])
      }catch {
        return nil
      }//end do
    }//end get
  }//end member

  public var asFileStatuses: [FileStatus] {
    get {
      do {
        let dic = try self.jsonDecode() as? [String:Any] ?? [:]
        return (dic["FileStatuses"] as? [Any] ?? []).map { FileStatus($0 as? [String:Any] ?? [:]) }
      }catch {
        return []
      }//end do
    }//end get
  }//end member

  public var asLong: Int {
    get {
      do {
        let dic = try self.jsonDecode() as? [String:Any] ?? [:]
        return dic["long"] as? Int ?? 0
      } catch {
        return 0
      }//end do
    }//end get
  }//end asBoolean

  public var asPath: String {
    get {
      do {
        let dic = try self.jsonDecode() as? [String:Any] ?? [:]
        return dic["Path"] as? String ?? ""
      } catch {
        return ""
      }//end do
    }//end get
  }//end asBoolean
}//end String

public struct RemoteException{
  var javaClassName = ""
  var exception = ""
  var message = ""
  public init(_ dictionary: [String:Any] = [:]) {
    self.javaClassName = dictionary["javaClassName"] as? String ?? ""
    self.exception = dictionary["exception"] as? String ?? ""
    self.message = dictionary["message"] as? String ?? ""
  }//init
}//Exception

extension String {
  public var asRemoteException: RemoteException? {
    get {
      do {
        let dic = try self.jsonDecode() as? [String:Any] ?? [:]
        return RemoteException(dic["RemoteException"] as? [String:Any] ?? [:])
      }catch {
        return nil
      }//end do
    }//end get
  }//end member
}//end class
