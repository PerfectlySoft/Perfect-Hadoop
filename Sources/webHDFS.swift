//
//  webHDFS.swift
//  PerfectHadoop
//
//  Created by Rockford Wei on 2016-11-04.
//	Copyright (C) 2015 PerfectlySoft, Inc.
//
//===----------------------------------------------------------------------===//
//
// This source file is part of the Perfect.org open source project
//
// Copyright (c) 2016 - 2017 PerfectlySoft Inc. and the Perfect project authors
// Licensed under Apache License v2.0
//
// See http://perfect.org/licensing.html for license information
//
//===----------------------------------------------------------------------===//
//

import cURL
import PerfectCURL
import PerfectLib
import Foundation

/// WebHDFS - hadoop connector for Perfect Framework
public class WebHDFS {

  /// Web Protocol, may be http / https / hdfs or webhdfs
  private var service: String = "http"

  /// URL hostname or IP address
  private var host: String = "localhost"

  /// URL port to connect
  private var port: Int = 9870


  /// Authentication Model of HDFS
  public enum Authentication {

    /// Authentication when security is off, even without a user name
    case off

    /// Authentication when security is off
    /// - parameters:
    ///   - name: user.name for request
    case byUser(name: String)

    /// Authentication using Kerberos SPNEGO when security is on
    /// *NOTE*: users MUST call kinit / kdestroy themselves (user login/logoff)
    /// curl --negotiate will apply
    /// - parameters:
    ///   - name: user.name for request
    case byKerberos(name: String)
  }// end Authentication

  /// Authentication of request
  private var auth: Authentication = .off

  /// Proxy User, if applicable
  private var proxyUser: String = ""

  /// Extra headers, such as X-XSRF-HEADER: "",
  /// for Cross-Site Request Forgery Prevention, if applicable
  private var extraHeaders: [String] = []

  /// cURL agent, must be set for some of the system.
  private let agent: String = "PerfectCURL"

  /// api base, depends on system
  private var base: String = "/webhdfs/v1"

  /// timeout in milliseconds. default zero means it will never timeout.
  private var timeout: Int = 0

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
  }//end enum

  /// common methods used by webhdfs
  public enum WebHdfsMethod {

    /// default get method
    case GET

    /// for upload a file
    case PUT

    /// for updating information
    case POST

    /// for removing or erasing a file / directory
    case DELETE
  }//end enum

  /*
   having trouble in convension, temporarily suspended.
   public enum HdfsFileType:String {
   case FILE = "FILE"
   case DIRECTORY = "DIRECTORY"
   case SYMLINK = "SYMLINK"
   }//end enum
   */

  /// Abstract Class for Structure auto-wrapper
  public class JSONStruct : NSObject {

    /// constructor
    /// - parameters
    ///   - dictonary: a decoded json structure
    public init(dictionary:[String:Any]) throws {

      super.init()
      for (key, value) in dictionary {
        self.setValue(value, forKey: key)
      }//next
    }//end constructor
  }

  /// common file status parsed from json
  public class HdfsFileStatus : JSONStruct {

    /// file suffix / extension - type
    var pathSuffix: String = ""

    /// storage unit, default = 128M, min = 1M
    var blockSize: ULONG = 0

    /// replicated nodes count
    var replication: Int = 0

    /// node type: directory or file
    var type: String = ""

    /// unix time for last access
    var accessTime: ULONG = 0

    /// number of children - for directory only
    var childrenNum: Int = 0

    /// hdfs storage policy
    var storagePolicy: Int = 0

    /// every node has a unique id, hopefully.
    var fileId: Int = 0

    /// node owner
    var owner: String = ""

    /// last modification in unix time format
    var modificationTime: ULONG = 0

    /// node group info
    var group: String = ""

    /// node permission, (u)rwx (g)rwx (o)rwx
    var permission: Int = 0

    /// file length
    var length: ULONG = 0

  }//end class

  /// directory content summary parsed from json
  public class DirectoryContentSummary : JSONStruct {

    // how many sub folders does this node have
    var directoryCount: Int = 0

    // file count of the node
    var fileCount: Int = 0

    // length of a node?
    var length: ULONG = 0

    // quota of the node
    var quota: Int  = -1

    // blocks that node consumed
    var spaceConsumed: ULONG = 0

    // block quota
    var spaceQuota: Int = -1

    // other quota
    var typeQuota: [String:[String:Any]] = [:]
  }//end class


  /// file checksum parsed from json
  public class FileCheckSum : JSONStruct {

    // algorithm information of this checksum
    var algorithm : String = ""

    // check sum content
    var bytes : String = ""

    // length of the checksum content
    var length: Int = 0
  }//end class

  /// ACL status class
  public class AclStatus: JSONStruct {
    // ACL entries
    var entries: [String] = []
    var group: String = ""
    var owner: String = ""
    var permission: Int = 775
    var stickyBit: Bool = false
  }//end class

  /// XAttr class - attribute extension
  public class XAttr: JSONStruct {

    // name of the attribute
    var name: String = ""

    // value of the attribute
    var value: String = ""
  }//end XAttr

  /// constructor of webhdfs
  /// - parameters:
  ///   - service: the protocol of web request - http / https / webhdfs / hdfs
  ///		- host: the hostname or ip address of the webhdfs host
  ///		- port: the port of webhdfs host
  ///		- auth: Authorization Model. Please check the enum Authentication for details
  ///   - proxyUser: proxy user, if applicable
  ///   - extraHeaders: extra headers if special header such as CSRF (Cross-Site Request Forgery Prevention) is applicable
  ///		- apibase: use this parameter ONLY the target server has a different api routine other than /webhdfs/v1
  ///   - timeout: timeout in seconds, zero means never timeout during transfer
  public init (service: String = "http",
               host: String = "localhost", port: Int = 9870,
               auth: Authentication = .off, proxyUser: String = "",
               extraHeaders: [String] = [],
               apibase: String = "/webhdfs/v1", timeout: Int = 0) {
    self.service = service
    self.host = host
    self.port = port
    self.auth = auth
    self.extraHeaders = extraHeaders
    self.proxyUser = proxyUser
    self.base = apibase
    self.timeout = timeout
  }//end constructor

  ///	quick conversion from bool to string
  /// - parameter:
  ///   - variable: boolean value to convert
  @discardableResult
  private func boolStr(_ variable: Bool) -> String {
    return variable ? "true" : "false"
  }

  ///	quick conversion from a json decode result
  /// - parameter:
  ///	json string which contains the result
  @discardableResult
  private func boolResult(_ variable: [String:Any]) -> Bool {
    return (variable["boolean"] as? Bool)!
  }

  /// generate a url by the provided information
  /// - returns:
  ///	the assembled url
  ///	- parameters:
  ///		- operation: each webhdfs API has a unique operation string, such as CREATE / GETFILESTATUS, etc.
  ///		- path: full path of the objective file / directory.
  ///		- variables: further options to complete this operation
  @discardableResult
  private func assembleURL(_ operation: String, _ path: String, _ variables:[String:Any] = [:]) -> String {
    // assamble the url path
    var url = "\(service)://\(host):\(port)\(base)\(path)?op=\(operation)"

    // join all variables into a string
    if (variables.count > 0) {
      let par = variables.reduce("") { $0 + "&\($1.key)=\($1.value)"}
      url.append(par)
    }//end if

    switch self.auth {
    case .byUser(let name), .byKerberos(let name) :
      if !name.isEmpty {
        url.append("&user.name=\(name)")
    }//end if
    default: ()
    }//end switch

    if !proxyUser.isEmpty {
      url.append("&doas=\(proxyUser)")
    }//end proxy

    return url
  }//end assembleURL

  /// perform a cURL request
  /// - returns:
  ///	(header, body as String, body as json, and body as [UInt8])
  ///	header is the response header string
  /// Three different forms of body data: binary bytes, string or decoded json
  ///
  ///	- parameters:
  ///		- operation: each webhdfs API has a unique operation string, such as CREATE / GETFILESTATUS, etc., can be empty as default if only overwriteURL is set
  ///		- path: full path of the objective file / directory, can be empty if only overwriteURL is not.
  ///		- variables: further options to complete this operation
  ///		- method: http method, default is GET
  ///   - overwriteURL: use this parameter to overwrite url assembled by (operation + path + veriables)
  ///   - decode: If false, the perform will neither decode it as json nor assemble the data into an ASCII string.
  ///   - moreOptions: a closure that allows set the curl object with more options
  @discardableResult
  internal func perform(operation:String = "", path:String = "", variables:[String: Any] = [:], method: WebHdfsMethod = .GET, overwriteURL:String = "", decode: Bool = true, moreOptions: ((CURL)->Void)? = { _ in }) throws -> (String, String, [String:Any], [UInt8]) {

    // get the full url string
    let url = overwriteURL.isEmpty ? assembleURL(operation, path, variables) : overwriteURL

    // initialize a curl request object
    let curl = CURL(url: url)
    defer {
      curl.close()
    }//end defer

    // some webhdfs servers require the user agent infor, such as ie. & safari
    let _ = curl.setOption(CURLOPT_USERAGENT, s: agent)

    // set timeout in milliseconds
    if (timeout>0) {
      let _ = curl.setOption(CURLOPT_TIMEOUT, int: timeout)
    }//end if

    // choose the proper option for each method
    switch(method) {
    case .PUT:
      let _ = curl.setOption(CURLOPT_UPLOAD, int: 1)
    case .POST:
      let _ = curl.setOption(CURLOPT_POST, int: 1)
    case .DELETE:
      let _ = curl.setOption(CURLOPT_CUSTOMREQUEST, s: "DELETE")
    default:
      let _ = curl.setOption(CURLOPT_HTTPGET, int: 1)
    }//end switch

    // set the curl to negotiate with Kerberos server.
    switch auth {

    // *NOTE* kerberos doesn't require a user name / password to negotiate
    // it has been done by kinit *OUTSIDE* this program
    case .byKerberos( _ ):

      // *NOTE* Perfect libCURL exports don't include this option,
      // assign the value manually.
      let CURLAUTH_NEGOTIATE = 4

      // set negotiation
      let _ = curl.setOption(CURLOPT_HTTPAUTH, int: CURLAUTH_NEGOTIATE)

      // NOTE
      let _ = curl.setOption(CURLOPT_USERPWD, s: ":")
    default: ()
    }//end case

    // create a handle for these headers
    var headerList:UnsafeMutablePointer<curl_slist>? = nil

    // if there are extr headers
    if !extraHeaders.isEmpty {

      // iterate all extra headers
      for item in extraHeaders {

        // save them into the handle
        headerList = curl_slist_append(headerList, item)

      }//next

      // pass the handle pointer to curl
      curl.setOption(CURLOPT_HTTPHEADER, v: headerList!)
    }//end if

    // if there are more options, call them.
    if let callbackOptions = moreOptions {

      // execute the callback
      callbackOptions(curl)
    }//end if

    // block the thread until timeout
    let (code, header, body ) = curl.performFully()

    guard code > -1 else {
      throw Exception.unexpectedResponse(url: url, header: "code = \(code)", body: "")
    }//end guard

    // release the header list immediately, if applicable
    if headerList != nil {

      // free them all
      curl_slist_free_all(headerList!)

    }//end if

    var headerStr: String = ""

    var content:String = ""

    var json: [String:Any] = [:]

    if body.count > 0 && decode {

      do {
        // assamble the body from a binary byte array to a string
        let str = String(bytes:body, encoding:String.Encoding.ascii)

        content = str!
        // decode the json string
        // it is possible that return value is not a json, so just simply ignore this error
        let data = try content.jsonDecode() as? [String : Any]

        json = data!
      } catch {}
    }//end if

    // if header is available
    guard header.count > 0 else {
      throw Exception.unexpectedResponse(url: url, header: "NULL HEADER", body: content)
    }//end guard

    headerStr = String(bytes: header, encoding: String.Encoding.ascii)!

    // may cases (redirection, authrization) will return more than one headers,
    // so keep the last one as the expected one.
    // make a NSString copy
    let hd = headerStr as NSString

    // find a pattern, something like HTTP/1.1 401 or 200
    let reg = try! NSRegularExpression(pattern: "(http|HTTP)\\/\\d.\\d\\s\\d{3}")

    // find different headers
    let res = reg.matches(in: headerStr, options:[], range:NSMakeRange(0, hd.length))

    // get the response code from each header
    let codes = res.map { m -> Int in
      let n = hd.substring(with: m.range).components(separatedBy: " ")
      return Int(n[1])!
    }//end codes

    guard codes[codes.count - 1] < 400 else {
      throw Exception.unexpectedResponse(url: url, header: headerStr, body: content)
    }//end guard

    return (headerStr, content, json, body)
  }//end perform

  /// parse relocation
  /// - returns:
  ///   String: redirected location url
  ///
  /// - parameters:
  ///   - header: header of the http response
  ///   - body: body of the http response
  ///   - json: json decoded from body
  ///   - throws:
  ///   - unexpected response
  @discardableResult
  private func relocation(header: String, body: String, json: [String:Any]) throws -> String {
    // hadoop 2.7.3
    if header.contains(string: "Location:") {
      let reg = try NSRegularExpression(pattern: "Location:\\s(.*)")
      let hd = header as NSString
      let results = reg.matches(in: header, range: NSRange(location:0, length:hd.length))
      let m = results.map { hd.substring(with: $0.range) }
      let n = m[0] as String
      return n.substring(from: (n.range(of: "http")?.lowerBound)!)
    }

    // hadoop 3.0+
    guard let location = json["Location"] as? String else {
        throw Exception.unexpectedResponse(url: "", header: header, body: body)
    }//end loc

    return location
  }//end relocation

  /// Open an hdfs file
  /// - returns:
  ///	[UInt8]: the file content both in bytes.
  ///
  ///	- parameters:
  ///		- path: full path of the remote file / directory.
  ///   - offset: The starting byte position.
  ///   - length: The number of bytes to be processed.
  ///   - buffersize: The size of the buffer used in transferring data.
  /// - throws:
  ///	Invalid file / unauthorized operation
  @discardableResult
  public func openFile(path:String, offset: Int = 0, length: Int = -1, buffersize: Int = 0) throws -> [UInt8] {

    var v: [String:Any] = [:]

    if offset > 0 {
      v["offset"] = offset
    }//end if

    if length > -1 {
      v["length"] = length
    }//end if

    if buffersize > 0 {
      v["buffersize"] = buffersize
    }//end if

    // retrieve the content in binary bytes
    let (_, _, _, body) = try perform(operation: "OPEN", path: path, variables: v, decode: false) {

      curl in

      // allow to redirect to data location
      let _ = curl.setOption(CURLOPT_FOLLOWLOCATION, int: 1)
    }//end perform

    return body
  }//end openFile

  /// Open an hdfs file
  /// - returns:
  ///	an instance of HdfsFileStatus class
  ///
  ///	- parameters:
  ///		- path: full path of the remote file / directory.
  /// - throws:
  ///	Invalid file
  @discardableResult
  public func getFileStatus(path:String) throws -> HdfsFileStatus {

    // perform a request
    let (_, _, dat, _) = try perform(operation: "GETFILESTATUS", path: path)

    // decode the json return
    let dic = dat["FileStatus"]! as? [String: Any]

    // convert the json into a readable structure
    return try HdfsFileStatus(dictionary: dic!)
  }//end getFileStatus

  /// List a Directory
  /// - returns:
  ///	[HdfsFileStatus]
  ///
  ///	- parameters:
  ///		- path: full path of the remote file / directory.
  /// - throws:
  ///	Invalid file
  @discardableResult
  public func listStatus(path:String) throws -> [HdfsFileStatus] {

    // perform a request
    let (_, _, dat, _) = try perform(operation: "LISTSTATUS", path: path)

    // decode the json return
    let statuses = dat["FileStatuses"] as? [String: Any]

    let dic = statuses?["FileStatus"] as? [Any]

    let list = try dic?.map { try HdfsFileStatus(dictionary: $0 as! [String:Any])}
    return list!
  }//end getFileStatus

  /// Get Content Summary of a Directory
  /// - returns:
  ///	DirectoryContentSummary
  ///
  ///	- parameters:
  ///		- path: full path of the remote file / directory.
  /// - throws:
  ///	Invalid file
  @discardableResult
  public func getDirectoryContentSummary(path:String) throws -> DirectoryContentSummary {

    // perform a request
    let (_, _, dat, _) = try perform(operation: "GETCONTENTSUMMARY", path: path)

    // decode the json return
    let content = dat["ContentSummary"] as? [String: Any]

    // turn the json into a real struct
    let summary = try DirectoryContentSummary(dictionary: content!)

    return summary
  }//end getDirectoryContentSummary

  /// Get File Checksum
  /// - returns:
  ///	FileCheckSum
  ///
  ///	- parameters:
  ///		- path: full path of the remote file / directory.
  /// - throws:
  ///	Invalid file
  @discardableResult
  public func getFileCheckSum(path:String) throws -> FileCheckSum {

    // perform a request
    let (_, _, dat, _) = try perform(operation: "GETFILECHECKSUM", path: path) {
      curl in
      curl.setOption(CURLOPT_FOLLOWLOCATION, int: 1)
    }//end more options

    // decode the json return
    let content = dat["FileChecksum"] as? [String: Any]

    // turn the json into a real struct
    let checksum = try FileCheckSum(dictionary: content!)

    return checksum
  }//end getFileCheckSum

  /// Get Home Directory
  /// - returns:
  ///	a string of path
  ///
  /// - throws:
  ///	Invalid file
  @discardableResult
  public func getHomeDirectory() throws -> String {

    // perform a request
    let (_, _, dat, _) = try perform(operation: "GETHOMEDIRECTORY", path: "/")

    // decode the json return
    let path = dat["Path"] as? String

    return path!
  }//end getHomeDirectory

  /// Create a directory
  /// - returns:
  ///	true for a successful creation
  ///
  ///	- parameters:
  ///		- path: full path of the remote file / directory.
  ///		- permission: unix style file permission (u)rwx (g)rwx (o)rwx. Default is 755, i.e., rwxr-xr-x
  /// - throws:
  ///	unauthorized access
  @discardableResult
  public func mkdir(path:String, permission: Int = 755) throws -> Bool {

    // perform a request
    let (_, _, res, _) = try perform(operation: "MKDIRS", path: path, variables: ["permission": permission], method: .PUT)

    // return the decoded json result
    return boolResult(res)
  }//end mkdir

  /// Delete a file or a directory
  /// - returns:
  ///	true for a successful creation
  ///
  ///	- parameters:
  ///		- path: full path of the remote file / directory.
  ///		- recursive: delete (a directory) recursively.
  /// - throws:
  ///	unauthorized access / Invalid file or directory.
  @discardableResult
  public func delete(path:String, recursive: Bool = true) throws -> Bool {

    // perform a request
    let (_, _, res, _) = try perform(operation: "DELETE", path: path, variables: ["recursive": boolStr(recursive)], method: .DELETE)

    // return the decoded json result
    return boolResult(res)
  }//end mkdir

  /// Upload a file
  /// - returns:
  ///	true for a successful creation
  ///
  ///	- parameters:
  ///		- path: full path of the remote file / directory.
  ///		- localFile: full path of file to upload
  ///   - overwrite: If a file already exists, should it be overwritten?
  ///		- permission: unix style file permission (u)rwx (g)rwx (o)rwx. Default is 755, i.e., rwxr-xr-x
  ///		- blocksize: size of per block unit. default 128M, min = 1M
  ///		- replication: The number of replications of a file.
  ///		- buffersize: The size of the buffer used in transferring data.
  /// - throws:
  ///	unauthorized access / Invalid file or directory.
  @discardableResult
  public func create(path:String, localFile:String, overwrite: Bool = false, permission: Int = 644, blocksize: CLong = 1048576, replication: CShort = -1, buffersize: Int = -1) throws -> Bool {

    // prepare local file information
    var st = stat()

    // warning: st is an unsafe pointer
    let statRes = stat(localFile, &st)

    // if file doesn't exist
    guard statRes != -1 else {
      throw Exception.invalidLocalFile(reason: "Invalid Path")
    }

    // ensure the file size is at least 0 (for a mark reason)
    guard st.st_size > -1 else {
      throw Exception.invalidLocalFile(reason: "Invalid Size")
    }

    // perpare the parameters
    var options : [String : Any] = ["overwrite": boolStr(overwrite), "permission": permission, "noredirect": boolStr(true)]
    if blocksize > 1048576 { options["blocksize"] = blocksize }
    if replication > -1 { options["replication"] = replication }
    if buffersize > -1 { options["buffersize"] = buffersize }

    // first step to retrieve the real data location (redirection)
    let (header, body, res, _) = try perform(operation: "CREATE", path: path, variables: options, method: .PUT)

    // get the redirection url
    let location = try relocation(header: header, body: body, json: res)

    // prepare file pointer to curl setOption. CAUTION: low-level operation with an unsafe pointer
    let fpointer = fopen(localFile, "rb")

    // MUST check the file info again!
    guard fpointer != nil else {
      throw Exception.invalidLocalFile(reason: "Open File Failure")
    }

    let _ = try perform(method: .PUT, overwriteURL: location) {
      curl in
      // map the upload call back function to a file reader
      let _ = curl.setOption(CURLOPT_READFUNCTION, f:{

        // parameters of READFUNCTION, i.e., identically the same style with fread()
        (buffer, size, nitems, instream) in

        // This is ONLY avaiable in swift 3.0, to cast a pointer from one type to another
        let fp = unsafeBitCast(instream, to: UnsafeMutablePointer<FILE>.self)

        // set the reading function
        return fread(buffer, size, nitems, fp)

      })//end setOption


      // set the option to upload
      let _ = curl.setOption(CURLOPT_UPLOAD, int: 1)

      // save the file pointer to curl request
      let _ = curl.setOption(CURLOPT_READDATA, v: fpointer!)

      // declare the file size
      let _ = curl.setOption(CURLOPT_INFILESIZE_LARGE, int: st.st_size)
    }

    // close the unsafe low level file pointer nicely.
    fclose(fpointer)

    // response will send a code of 201 if success.
    return true
  }//end mkdir

  /// Upload a file and append to the remote file
  /// - returns:
  ///	true for a successful creation
  ///
  ///	- parameters:
  ///		- path: full path of the remote file / directory.
  ///		- localFile: full path of file to upload
  ///		- buffersize: The size of the buffer used in transferring data.
  /// - throws:
  ///	unauthorized access / Invalid file or directory.
  @discardableResult
  public func append(path:String, localFile:String, buffersize: Int = -1) throws -> Bool {

    // prepare local file information
    let file = File(localFile)

    // if file doesn't exist
    guard file.exists else {
      throw Exception.invalidLocalFile(reason: "Invalid Path")
    }//end

    // check the file size before operation
    let size = file.size

    // ensure the file size is at least 1
    guard size > 0 else {
      throw Exception.invalidLocalFile(reason: "Invalid Size")
    }//end guard

    // get the firle content in bytes
    try file.open(.read)

    // read the whole file content as binary byte
    var content = try file.readSomeBytes(count: size)

    // nicely close after reading
    file.close()

    // perpare the parameters
    let options : [String : Any] = ["noredirect": boolStr(true)]

    // first step to retrieve the real data location (redirection)
    let (header, body, res, _) = try perform(operation: "APPEND", path: path, variables: options, method: .POST)

    // get the redirection url
    let location = try relocation(header: header, body: body, json: res)


    let _ = try perform(method: .POST, overwriteURL: location) {
      curl in
      // set the option to post
      let _ = curl.setOption(CURLOPT_POST, int: 1)

      // declare the file size
      let _ = curl.setOption(CURLOPT_POSTFIELDSIZE, int: size)

      // save the file pointer to curl request
      let _ = withUnsafeMutablePointer(to: &content, {
        let _ = curl.setOption(CURLOPT_POSTFIELDS, v: $0)
      }) //end mutable pointer
    }//end more options

    return true
  }//end mkdir

  /// concat the current path with one or more files
  /// - returns:
  ///	true for success
  ///
  ///	- parameters:
  ///		- path: full path of the remote file / directory.
  ///		- sources: A list of source paths.
  /// - throws:
  ///	unauthorized access / Invalid file or directory / Unsupport Operation.
  @discardableResult
  public func concat(path:String, sources:[String]) throws -> Bool {

    // check the file names before operation
    guard sources.count > 0 else {
      throw Exception.insufficientParameters
    }//end guard

    // join all file names into a string as required
    let files = sources.joined(separator: ",")

    // perform a request
    let _ = try perform(operation: "CONCAT", path: path, variables: ["sources": files], method: .POST)

    // test if success
    return true
  }//end concat

  /// create a symbol link of file. *NOTE* THIS FUNCTION MAY NOT BE SUPPORTED BY SOME HDFS SYSTEM.
  /// - returns:
  ///	true for a successful creation
  ///
  ///	- parameters:
  ///		- path: full path of the remote file / directory.
  ///		- destination: full path of the symbolic link to create
  ///		- createParent: create full path even parent path does not exist
  /// - throws:
  ///	unauthorized access / Invalid file or directory / Unsupport Operation.
  @discardableResult
  public func createSymLink(path: String, destination: String, createParent: Bool) throws -> Bool {

    // fill up the variables
    let v = ["destination": destination, "createParent": boolStr(createParent)]

    // perform a request
    let _ = try perform(operation: "CREATESYMLINK", path: path, variables:v, method:.PUT)

    // everything shall be fine, otherwise an error may throw before this return.
    return true
  }//end createSymlLink

  /// truncate a file
  /// - returns:
  ///	true for a successful creation
  ///
  ///	- parameters:
  ///		- path: full path of the remote file / directory.
  ///		- newlength: new length of the file
  /// - throws:
  ///	unauthorized access / Invalid file or directory / Unsupport Operation.
  @discardableResult
  public func truncate(path: String, newlength:CLong) throws -> Bool {

    // check if the length is valid
    guard newlength >= 0 else {
      throw Exception.unsupportedOperation(of: "file length less than zero")
    }//end guard

    // fill up the variables
    let v = ["newlength": newlength]

    // perform the request with more options
    let _ = try perform(operation: "TRUNCATE", path: path, variables:v, method:.POST) {
      curl in
      // set the option to post
      let _ = curl.setOption(CURLOPT_POST, int: 1)

      // declare the file size
      let _ = curl.setOption(CURLOPT_POSTFIELDSIZE, int: newlength)

      // save the file pointer to curl request, NOTE: it is a stupid bug of CURL, you will have to assign a not null pointer to the POSTFIELDS even it is useless.
      var content:String = "0"
      let _ = withUnsafeMutablePointer(to: &content, {
        let _ = curl.setOption(CURLOPT_POSTFIELDS, v: $0)
      })//end pointer
    }//end more options

    // everything shall be fine, otherwise an error may throw before this return.
    return true
  }//end truncate

  /// set permission
  /// - returns:
  /// true for a successful setting
  ///
  /// - parameters
  ///   - path: full path of the remote file / directory
  ///   - permission: oct permission of (u)rwx(g)rwx(o)rwx
  /// - throws
  /// unauthorized access / Invalid file or directory
  @discardableResult
  public func setPermission(path: String, permission: Int) throws -> Bool {
    guard permission >= 0 else {
      throw Exception.unsupportedOperation(of: "invalid permission oct")
    }//end guard

    let _ = try perform(operation: "SETPERMISSION", path:path, variables: ["permission": permission], method:.PUT)

    return true
  }//end setPermission

  /// set owner
  /// - returns:
  /// true for a successful setting
  ///
  /// - parameters
  ///   - path: full path of the remote file / directory
  ///   - owner: owner's name
  ///   - group: group's name
  /// - throws
  /// unauthorized access / Invalid file or directory
  @discardableResult
  public func setOwner(path: String, name: String, group:String) throws -> Bool {

    let _ = try perform(operation: "SETOWNER", path:path, variables: ["owner": name, "group": group], method:.PUT)

    return true
  }//end setOwner

  /// set replication factor
  /// - returns:
  /// true for a successful setting
  ///
  /// - parameters
  ///   - path: full path of the remote file / directory
  ///   - replication: replication factor
  /// - throws
  /// unauthorized access / Invalid file or directory
  @discardableResult
  public func setReplication(path: String, factor:ushort) throws -> Bool {

    // perform the test
    let (_,_,res,_) = try perform(operation: "SETREPLICATION", path:path, variables: ["replication": factor], method:.PUT)

    return boolResult(res)
  }//end setReplication

  /// set times
  /// - returns:
  /// true for a successful setting
  ///
  /// - parameters
  ///   - path: full path of the remote file / directory
  ///   - modification: modification time of the file / directory
  ///   - access: access time of the file / directory
  /// - throws
  /// unauthorized access / Invalid file or directory
  @discardableResult
  public func setTimes(path: String, modification: Int = -1, access: Int = -1) throws -> Bool {
    // prepare the variables
    var times = [String:Any]()

    // load modification time
    if modification > -2 {
      times["modificationtime"] = modification
    }//end if

    // load access time
    if access > -2 {
      times["accesstime"] = access
    }//end if

    // perform the test
    let _ = try perform(operation: "SETTIMES", path:path, variables: times, method:.PUT)

    return true
  }//end setTimes

  /// get ACL
  /// - returns:
  /// AclStatus
  ///
  /// - parameters
  ///   - path: full path of the remote file / directory
  @discardableResult
  public func getACL(path: String) throws -> AclStatus {
    // perform a request
    let (_, _, res, _) = try perform(operation: "GETACLSTATUS", path: path)

    // decode the json
    let json = res["AclStatus"] as? [String: Any]

    // wrap up with structure
    let acl = try AclStatus(dictionary: json!)

    return acl
  }//end getACL


  /// set ACL
  /// - returns:
  /// true for a successful modification
  ///
  /// - parameters
  ///   - path: full path of the remote file / directory
  ///   - specification: ACL spec string, such as "user::rwx,user:user1:---,group::rwx,other::rwx"
  /// - throws
  /// unauthorized access / Invalid file or directory
  @discardableResult
  public func setACL(path: String, specification: String) throws -> Bool {

    // perform the test
    let _ = try perform(operation: "SETACL", path:path, variables: ["aclspec":specification], method:.PUT)

    return true
  }//end setACL

  /// modify ACL entries
  /// - returns:
  /// true for a successful modification
  ///
  /// - parameters
  ///   - path: full path of the remote file / directory
  ///   - entries: ACL spec string, such as "user::rwx,user:user1:---,group::rwx,other::rwx"
  /// - throws
  /// unauthorized access / Invalid file or directory
  @discardableResult
  public func modifyACL(path: String, entries: String) throws -> Bool {

    // perform the test
    let _ = try perform(operation: "MODIFYACLENTRIES", path:path, variables: ["aclspec":entries], method:.PUT)

    return true
  }//end modifyACL

  /// remove ACL entries
  /// - returns:
  /// true for a successful modification
  ///
  /// - parameters
  ///   - path: full path of the remote file / directory
  ///   - entries: ACL spec string, such as "user::rwx,user:user1:---,group::rwx,other::rwx"
  /// - throws
  /// unauthorized access / Invalid file or directory
  @discardableResult
  public func removeACL(path: String, entries: String = "", defaultACL:Bool = false) throws -> Bool {

    if defaultACL {
      let _ = try perform(operation: "REMOVEDEFAULTACL", path:path, method:.PUT)
    } else {
      if entries.isEmpty {
        let _ = try perform(operation: "REMOVEACL", path:path, method:.PUT)
      } else {
        let _ = try perform(operation: "REMOVEACLENTRIES", path:path, variables: ["aclspec": entries], method:.PUT)
      }//end if
    }//end if
    return true
  }//end removeACL


  /// check access
  /// - returns:
  /// true for success
  /// - parameters:
  ///   - fsaction: file system action, open / append, etc.
  /// - throws
  /// unexpectedResponse
  @discardableResult
  public func checkAccess(path: String, fsaction: String) throws -> Bool {
    let _ = try perform(operation: "CHECKACCESS", path:path, variables:["fscation": fsaction])
    return true
  }//end checkAccess

  public enum XAttrFlag:String {
    case CREATE = "CREATE"
    case REPLACE = "REPLACE"
  }
  /// set xattr
  /// - returns:
  /// true for success
  ///
  /// - parameters:
  ///   - path: full path of the remote file / directory
  ///   - name: xattr name
  ///   - value: xattr value
  ///   - flag: xattr flag, only valid for CREATE OR REPLACE
  /// - throws:
  /// unauthorized access / Invaid file or directory
  @discardableResult
  public func setXAttr(path: String, name: String, value: String, flag: XAttrFlag = .CREATE) throws -> Bool {

    // perform a request
    let _ = try perform(operation: "SETXATTR", path: path, variables: ["xattr.name": name, "xattr.value": value, "flag": flag.rawValue], method:.PUT)

    return true
  }//end setXAttr

  /// remove xattr
  /// - returns:
  /// true for success
  ///
  /// - parameters:
  ///   - path: full path of the remote file / directory
  ///   - name: xattr name
  /// - throws:
  /// unauthorized access / Invaid file or directory
  @discardableResult
  public func removeXAttr(path: String, name: String) throws -> Bool {

    // perform a request
    let _ = try perform(operation: "REMOVEXATTR", path: path, variables: ["xattr.name": name], method:.PUT)

    return true
  }//end removeXAttr

  /// get xattr
  /// - returns:
  /// XAttr
  ///
  /// - parameters:
  ///   - path: full path of the remote file / directory
  ///   - name: xattr name array. by default [] to get all attributes
  ///   - encoding: value encoding
  /// - throws:
  /// unauthorized access / Invaid file or directory
  @discardableResult
  public func getXAttr(path: String, name: [String] = [], encoding: String = "") throws -> [XAttr] {

    var url = assembleURL("GETXATTRS", path)

    // set the variables
    if name.count > 0 {
      let names = name.map { "xattr.name=\($0)" } .joined(separator: "&")
      url.append("&" + names)
    }//end if

    // add a flag if need
    if !encoding.isEmpty {
      url.append("&encoding=\(encoding)")
    }//end if


    // perform a request
    let (_, _, res, _) = try perform(overwriteURL: url)

    // decode the json return
    let dic = res["XAttrs"] as? [Any]

    let list = try dic?.map { try XAttr(dictionary: $0 as! [String:Any])}
    return list!
  }//end getXAttr

  /// list all xattrs
  /// - returns:
  /// [String] - all attributes' name in an array
  ///
  /// - parameters:
  ///   - path: full path of the remote file / directory
  /// - throws:
  /// unauthorized access / Invaid file or directory
  @discardableResult
  public func listXAttr(path: String) throws -> [String] {

    // perform a request
    let (_, _, res, _) = try perform(operation: "LISTXATTRS", path:path)

    // decode the json return
    let json = res["XAttrNames"] as? String

    // is it a bug??? why do we need to decode it again???
    let list = try json?.jsonDecode() as? [String]

    return list!
  }//end listXAttr

  /// create snapshot *NOTE* directory must be set to snapshottable by admin
  /// - returns:
  /// (String, String) - snapshot full path and its short name
  ///
  /// - parameters:
  ///   - path: full path of the remote file / directory
  ///   - name: snapshot name
  /// - throws:
  /// unauthorized access / Invaid file or directory
  @discardableResult
  public func createSnapshot(path: String) throws -> (String, String) {

    // perform a request
    let (_, _, res, _) = try perform(operation: "CREATESNAPSHOT", path:path, method: .PUT)

    // decode the json return
    let shot = res["Path"] as? String

    let array = shot?.characters.split(separator: "/").map(String.init)
    let name = array?[(array?.count)! - 1]
    return (shot!, name!)
  }//end createSnapshot

  /// delete snapshot
  /// - returns:
  /// true for success
  ///
  /// - parameters:
  ///   - path: full path of the remote file / directory
  ///   - name: snapshot name
  /// - throws:
  /// unauthorized access / Invaid file or directory
  @discardableResult
  public func deleteSnapshot(path: String, name: String) throws -> Bool {

    // perform a request
    let _ = try perform(operation: "DELETESNAPSHOT", path:path, variables: ["snapshotname": name], method: .DELETE)

    return true
  }//end deleteSnapshot

  /// rename snapshot
  /// - returns:
  /// true for success
  ///
  /// - parameters:
  ///   - path: full path of the remote file / directory
  ///   - from: snapshot original name
  ///   - to: new name of the snapshot
  /// - throws:
  /// unauthorized access / Invaid file or directory
  @discardableResult
  public func renameSnapshot(path: String, from: String, to: String) throws -> Bool {

    // perform a request
    let _ = try perform(operation: "RENAMESNAPSHOT", path:path, variables: ["oldsnapshotname": from, "snapshotname":to], method: .PUT) {
      curl in

      curl.setOption(CURLOPT_FOLLOWLOCATION, int: 1)
    }

    return true
  }//end renameSnapshot
  /*
  /// get delegation token
  /// - returns
  /// String for token
  ///
  /// - parameters:
  ///   - renewer: The username of the renewer of a delegation token.
  ///   - service: The name of the service where the token is supposed to be used, e.g. ip:port of the namenode
  ///   - kind: The kind of the delegation token requested, e.g “HDFS_DELEGATION_TOKEN” or “WEBHDFS delegation”
  /// - throws:
  ///   unsupportedOperation
  @discardableResult
  public func getDelegationToken (renewer: String, service: String = "", kind: String = "") throws -> String {

    switch auth {
    case .byDelegation( _ ):
      // generate the basic url
      var url = assembleURL("GETDELEGATIONTOKEN", "/")

      // append renewer info
      url.append("&renewer=\(renewer)")

      // append the service if available
      if !service.isEmpty {
        url.append("&service=\(service)")
      }//end if

      // append the kind if available
      if !kind.isEmpty {
        url.append("&kind=\(kind)")
      }//end if

      let (_,_,res,_) = try perform(overwriteURL:url)  {
        curl in
        // allow redirection
        curl.setOption(CURLOPT_FOLLOWLOCATION, int: 1)
      }//end more options

      let token = res["Token"] as? String
      return token ?? ""

    default:
      throw WebHdfsError.unsupportedOperation(of: "Get Delegation Token without Authentication")
    }//end case

  }//end getDelegationToken

  /// renew delegation token
  /// - returns:
  /// long for the new expiration time
  /// - throws:
  /// unsupportedOperation
  @discardableResult
  public func renewDelegationToken() throws -> Int32 {

    switch auth {
    case .byDelegation(let token):
      // generate the url
      var url = assembleURL("RENEWDELEGATIONTOKEN", "/")

      // set the token string into url
      url.append("&token=\(token)")

      // call the api
      let (_, _, res, _) = try perform(method:.PUT, overwriteURL: url) {
        curl in
        // allow redirection
        curl.setOption(CURLOPT_FOLLOWLOCATION, int: 1)
      }//end more options


      // parse the result
      let expiration = res["long"] as? NSString

      // return the new expiration time
      return (expiration?.intValue)!
    default:
      throw WebHdfsError.unsupportedOperation(of: "Renew Token without Authentication")
    }//end auth
  }//end renew

  /// renew delegation token
  /// - returns:
  /// long for the new expiration time
  /// - throws:
  /// unsupportedOperation
  @discardableResult
  public func cancelDelegationToken() throws -> Bool {

    switch auth {
    case .byDelegation(let token):
      // generate the url
      var url = assembleURL("CANCELDELEGATIONTOKEN", "/")

      // set the token string into url
      url.append("&token=\(token)")

      // call the api
      let _ = try perform(method:.PUT, decryption: false, overwriteURL: url)  {
        curl in

        // allow redirection
        curl.setOption(CURLOPT_FOLLOWLOCATION, int: 1)
      }//end more options
      return true
    default:
      throw WebHdfsError.unsupportedOperation(of: "Cancel Token without Authentication")
    }//end case
  }//end cancel
 */
}//end class
