//
//  WebHDFS.swift
//  PerfectHadoop
//
//  Created by Rockford Wei on 2016-12-20.
//	Copyright (C) 2016 PerfectlySoft, Inc.
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

/// WebHDFS - hadoop connector for Perfect Framework
public class WebHDFS {

  /// Web Protocol, may be http / https / hdfs or webhdfs
  internal var service: String = "http"

  /// URL hostname or IP address
  internal var host: String = "localhost"

  /// URL port to connect
  internal var port: Int = 9870

  /// User Name - not required by default.
  internal var user: String = ""

  /// Authentication Model of HDFS
  public enum Authentication {

    /// Authentication when security is off, even without a user name
    case off

    /// Authentication using Kerberos SPNEGO when security is on
    case krb5
  }// end Authentication

  /// Authentication of request
  internal var auth: Authentication = .off

  /// Proxy User, if applicable
  internal var proxyUser: String = ""

  /// Extra headers, such as X-XSRF-HEADER: "",
  /// for Cross-Site Request Forgery Prevention, if applicable
  internal var extraHeaders: [String] = []

  /// cURL agent, must be set for some of the system.
  internal let agent: String = "PerfectCURL"

  /// api base, depends on system
  internal var base: String = "/webhdfs/v1"

  /// timeout in milliseconds. default zero means it will never timeout.
  internal var timeout: Int = 0

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

  /// common methods used by webhdfs
  public enum HTTPMethod {

    /// default get method
    case GET

    /// for upload a file
    case PUT

    /// for updating information
    case POST

    /// for removing or erasing a file / directory
    case DELETE
  }//end enum

  public struct Token {
    var urlString = ""
    public init(_ json: String = "") {
      do {
        let dic = try json.jsonDecode() as? [String:Any] ?? [:]
        self.urlString = dic["urlString"] as? String ?? ""
      }catch { }
    }//end init
  }//end Token

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
               host: String = "localhost", port: Int = 9870, user: String = "",
               auth: Authentication = .off, proxyUser: String = "",
               extraHeaders: [String] = [],
               apibase: String = "/webhdfs/v1", timeout: Int = 0) {
    self.service = service
    self.host = host
    self.port = port
    self.user = user
    self.auth = auth
    self.extraHeaders = extraHeaders
    self.proxyUser = proxyUser
    self.base = apibase
    self.timeout = timeout
  }//end constructor

  /// HTTP headers may contain over one HTTP code lines. This function will transform these lines into an array of responding codes
  /// - parameters:
  ///   - header: binary buffer of header
  /// - returns:
  ///   - header string as a whole
  ///   - an array of code number sequence
  @discardableResult
  internal func responseCodeSequence(_ header: [UInt8]) -> (String, [Int]) {
    // validate input
    if header.isEmpty {
      return ("", [])
    }//end if

    // assemble the header data into string first
    let headerStr = String(cString: header)

    // filter out all http strings
    let codes = headerStr.characters.split(separator: "\n").filter { str in
      // pick a line
      let s = String(str)
      // check if it starts with http
      return s.lowercased().hasPrefix("http")
    }.map { str->Int in
      // split it into two parts
      let turple = str.split(separator: " ")
      // if something wrong, return a zero
      if turple.count < 2 {
        return 0
      }else {
        // otherwise turn the string into a number
        return Int(String(turple[1])) ?? 0
      }//end if
    }.filter { $0 > 0 }

    // return the results
    return (headerStr, codes)
  }//end

  /// generate a url by the provided information
  /// - returns:
  ///	the assembled url
  ///	- parameters:
  ///		- operation: each webhdfs API has a unique operation string, such as CREATE / GETFILESTATUS, etc.
  ///		- path: full path of the objective file / directory.
  ///		- variables: further options to complete this operation
  @discardableResult
  internal func assembleURL(_ operation: String, _ path: String, _ variables:[String:Any] = [:]) -> String {
    // assamble the url path
    var url = "\(service)://\(host):\(port)\(base)\(path)?op=\(operation)"

    var varies = variables

    if !user.isEmpty {
      varies["user.name"] = user
    }//end if

    if !proxyUser.isEmpty {
      varies["doas"] = proxyUser
    }//end proxy

    // join all variables into a string
    if (varies.count > 0) {
      let par = varies.reduce("") { $0 + "&\($1.key)=\($1.value)"}
      url.append(par)
    }//end if

    return url
  }//end assembleURL

  /// perform a cURL request
  /// - returns:
  ///	(header, body as String and body as [UInt8])
  ///	header is the response header string
  /// Two different forms of body data: binary bytes or string
  ///
  ///	- parameters:
  ///		- operation: each webhdfs API has a unique operation string, such as CREATE / GETFILESTATUS, etc., can be empty as default if only overwriteURL is set
  ///		- path: full path of the objective file / directory, can be empty if only overwriteURL is not.
  ///		- variables: further options to complete this operation
  ///		- method: http method, default is GET
  ///   - overwriteURL: use this parameter to overwrite url assembled by (operation + path + veriables)
  ///   - decode: If false, the perform will not assemble the data into an ASCII string.
  ///   - moreOptions: a closure that allows set the curl object with more options
  @discardableResult
  internal func perform(operation:String = "", path:String = "", variables:[String: Any] = [:], method: HTTPMethod = .GET, overwriteURL:String = "", decode: Bool = true, moreOptions: ((CURL)->Void)? = { _ in }) throws -> (String, String, [UInt8]) {

    // get the full url string
    let url = overwriteURL.isEmpty ? assembleURL(operation, path, variables) : overwriteURL

    #if DEBUG
    print(url)
    #endif

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

    let _ = curl.setOption(CURLOPT_READFUNCTION, f: { _, _, _, _ in return 0})
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
    case .krb5:

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
    var headerList = UnsafeMutablePointer<curl_slist>(bitPattern: 0)

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

    var content:String = ""

    if !body.isEmpty && decode {

      // assamble the body from a binary byte array to a string
      content = String(cString: body)

    }//end if

    let (headerStr, codes) = responseCodeSequence(header)

    // if header is available
    guard !codes.isEmpty else {
      throw Exception.unexpectedResponse(url: url, header: "NULL HEADER", body: content)
    }//end guard

    guard codes[codes.count - 1] < 400 else {
      throw Exception.unexpectedResponse(url: url, header: headerStr, body: content)
    }//end guard

    return (headerStr, content, body)
  }//end perform

  /// parse relocation
  /// - returns:
  ///   String: redirected location url or nil
  ///
  /// - parameters:
  ///   - header: header of the http response
  ///   - body: body of the http response
  @discardableResult
  internal func relocation(header: String, body: String) -> String {
    // hadoop 2.7.3
    let prefix = "Location:"
    let locations = header.characters.split(separator: "\n")
      .filter { String($0).hasPrefix(prefix) }
      .map { String(String($0).characters.dropFirst(prefix.utf8.count)) }
    if locations.count > 0 {
      return locations[0]
    }//end if

    // hadoop 3.0+
    do {
      let location = try body.jsonDecode() as? [String:Any] ?? [:]
      return location["Location"] as? String ?? ""
    }catch {
      return ""
    }//end do
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
    let (_, _, body) = try perform(operation: "OPEN", path: path, variables: v, decode: false) {

      curl in

      // allow to redirect to data location
      let _ = curl.setOption(CURLOPT_FOLLOWLOCATION, int: 1)
    }//end perform

    return body
  }//end openFile

  /// Open an hdfs file
  /// - returns:
  ///	FileStatus
  ///	- parameters:
  ///		- path: full path of the remote file / directory.
  @discardableResult
  public func getFileStatus(path:String) throws -> FileStatus? {
    let (_, dat, _) = try perform(operation: "GETFILESTATUS", path: path)
    return dat.asFileStatus
  }//end getFileStatus

  /// List a Directory
  /// - returns:
  ///	[FileStatus]
  ///	- parameters:
  ///		- path: full path of the remote file / directory.
  @discardableResult
  public func listStatus(path:String) throws -> [FileStatus] {
    // perform a request
    let (_, dat, _) = try perform(operation: "LISTSTATUS", path: path)
    return dat.asFileStatuses
  }//end getFileStatus

  /// Get Content Summary of a Directory
  /// - returns:
  ///	ContentSummary
  ///	- parameters:
  ///		- path: full path of the remote file / directory.
  @discardableResult
  public func getDirectoryContentSummary(path:String) throws -> ContentSummary? {
    // perform a request
    let (_, dat, _) = try perform(operation: "GETCONTENTSUMMARY", path: path)
    return dat.asContentSummary
  }//end getDirectoryContentSummary

  /// Get File Checksum
  /// - returns:
  ///	FileCheckSum
  ///	- parameters:
  ///		- path: full path of the remote file / directory.
  @discardableResult
  public func getFileCheckSum(path:String) throws -> FileChecksum? {
    // perform a request
    let (_, dat, _) = try perform(operation: "GETFILECHECKSUM", path: path) {
      curl in
      curl.setOption(CURLOPT_FOLLOWLOCATION, int: 1)
    }//end more options
    return dat.asFileChecksum
  }//end getFileCheckSum

  /// Get Home Directory
  /// - returns:
  ///	a string of path
  @discardableResult
  public func getHomeDirectory() throws -> String {
    // perform a request
    let (_, dat, _) = try perform(operation: "GETHOMEDIRECTORY", path: "/")
    return dat.asPath
  }//end getHomeDirectory

  /// Create a directory
  ///	- parameters:
  ///		- path: full path of the remote file / directory.
  ///		- permission: unix style file permission (u)rwx (g)rwx (o)rwx. Default is 755, i.e., rwxr-xr-x
  /// - throws:
  ///	unauthorized access
  @discardableResult
  public func mkdir(path:String, permission: Int = 755) throws {
    // perform a request
    let (_, dat, _) = try perform(operation: "MKDIRS", path: path, variables: ["permission": permission], method: .PUT)
    if dat.asBoolean {
      return
    }//end if
    throw Exception.unexpectedReturn
  }//end mkdir

  /// Delete a file or a directory
  ///	- parameters:
  ///		- path: full path of the remote file / directory.
  ///		- recursive: delete (a directory) recursively.
  /// - throws:
  ///	unauthorized access / Invalid file or directory.
  @discardableResult
  public func delete(path:String, recursive: Bool = true) throws {

    // perform a request
    let (_, dat, _) = try perform(operation: "DELETE", path: path, variables: ["recursive": recursive.string], method: .DELETE)
    if dat.asBoolean {
      return
    }//end if
    throw Exception.unexpectedReturn
  }//end mkdir

  /// Upload a file
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
  public func create(path:String, localFile:String, overwrite: Bool = false, permission: Int = 644, blocksize: CLong = 1048576, replication: CShort = -1, buffersize: Int = -1) throws{

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
    var options : [String : Any] = ["overwrite": overwrite.string, "permission": permission, "noredirect": true.string]
    if blocksize > 1048576 { options["blocksize"] = blocksize }
    if replication > -1 { options["replication"] = replication }
    if buffersize > -1 { options["buffersize"] = buffersize }

    // first step to retrieve the real data location (redirection)
    let (header, body, _) = try perform(operation: "CREATE", path: path, variables: options, method: .PUT)

    // get the redirection url
    let newURL = relocation(header: header, body: body)

    guard !newURL.isEmpty else {
      throw Exception.unexpectedResponse(url: path, header: header, body: body)
    }//end guard

    // prepare file pointer to curl setOption. CAUTION: low-level operation with an unsafe pointer
    let fpointer = fopen(localFile, "rb")

    // MUST check the file info again!
    guard fpointer != nil else {
      throw Exception.invalidLocalFile(reason: "Open File Failure")
    }

    let _ = try perform(method: .PUT, overwriteURL: newURL) {
      curl in

      // set the option to upload
      let _ = curl.setOption(CURLOPT_UPLOAD, int: 1)

      // save the file pointer to curl request
      let _ = curl.setOption(CURLOPT_READDATA, v: fpointer!)

      // set the curl default read function
      let _ = curl.setOption(CURLOPT_READFUNCTION, f: { ptr, size, items, fpointer in
        guard fpointer != nil && ptr != nil else {
          return 0
        }//end guard
        return fread(ptr, size, items, unsafeBitCast(fpointer, to: UnsafeMutablePointer<FILE>.self))
      })
      
      // declare the file size
      let _ = curl.setOption(CURLOPT_INFILESIZE_LARGE, int: st.st_size)
    }

    // close the unsafe low level file pointer nicely.
    fclose(fpointer)
  }//end mkdir

  /// Upload a file and append to the remote file
  ///	- parameters:
  ///		- path: full path of the remote file / directory.
  ///		- localFile: full path of file to upload
  ///		- buffersize: The size of the buffer used in transferring data.
  /// - throws:
  ///	unauthorized access / Invalid file or directory.
  @discardableResult
  public func append(path:String, localFile:String, buffersize: Int = -1) throws {

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
    let options : [String : Any] = ["noredirect": true.string]

    // first step to retrieve the real data location (redirection)
    let (header, body, _) = try perform(operation: "APPEND", path: path, variables: options, method: .POST)

    // get the redirection url
    let newURL = relocation(header: header, body: body)

    guard !newURL.isEmpty else {
      throw Exception.invalidLocalFile(reason: "Expected relocation (nil)")
    }//end guard

    let _ = try perform(method: .POST, overwriteURL: newURL) {
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

  }//end mkdir

  /// concat the current path with one or more files
  ///	- parameters:
  ///		- path: full path of the remote file / directory.
  ///		- sources: A list of source paths.
  /// - throws:
  ///	unauthorized access / Invalid file or directory / Unsupport Operation.
  @discardableResult
  public func concat(path:String, sources:[String]) throws {

    // check the file names before operation
    guard sources.count > 0 else {
      throw Exception.insufficientParameters
    }//end guard

    // join all file names into a string as required
    let files = sources.joined(separator: ",")

    // perform a request
    let _ = try perform(operation: "CONCAT", path: path, variables: ["sources": files], method: .POST)
  }//end concat

  /// create a symbol link of file. *NOTE* THIS FUNCTION MAY NOT BE SUPPORTED BY SOME HDFS SYSTEM.
  ///
  ///	- parameters:
  ///		- path: full path of the remote file / directory.
  ///		- destination: full path of the symbolic link to create
  ///		- createParent: create full path even parent path does not exist
  /// - throws:
  ///	unauthorized access / Invalid file or directory / Unsupport Operation.
  @discardableResult
  public func createSymLink(path: String, destination: String, createParent: Bool) throws{

    // fill up the variables
    let v = ["destination": destination, "createParent": createParent.string]

    // perform a request
    let _ = try perform(operation: "CREATESYMLINK", path: path, variables:v, method:.PUT)

    // everything shall be fine, otherwise an error may throw before this return.
  }//end createSymlLink

  /// truncate a file
  ///	- parameters:
  ///		- path: full path of the remote file / directory.
  ///		- newlength: new length of the file
  /// - throws:
  ///	unauthorized access / Invalid file or directory / Unsupport Operation.
  @discardableResult
  public func truncate(path: String, newlength:CLong) throws {

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
  }//end truncate

  /// set permission
  /// - returns:
  /// - parameters
  ///   - path: full path of the remote file / directory
  ///   - permission: oct permission of (u)rwx(g)rwx(o)rwx
  /// - throws
  /// unauthorized access / Invalid file or directory
  @discardableResult
  public func setPermission(path: String, permission: Int) throws {
    guard permission >= 0 else {
      throw Exception.unsupportedOperation(of: "invalid permission oct")
    }//end guard

    let _ = try perform(operation: "SETPERMISSION", path:path, variables: ["permission": permission], method:.PUT)
  }//end setPermission

  /// set owner
  ///
  /// - parameters
  ///   - path: full path of the remote file / directory
  ///   - owner: owner's name
  ///   - group: group's name
  /// - throws
  /// unauthorized access / Invalid file or directory
  @discardableResult
  public func setOwner(path: String, name: String, group:String) throws {

    let _ = try perform(operation: "SETOWNER", path:path, variables: ["owner": name, "group": group], method:.PUT)
  }//end setOwner

  /// set replication factor
  /// - parameters
  ///   - path: full path of the remote file / directory
  ///   - replication: replication factor
  /// - throws
  /// unauthorized access / Invalid file or directory
  @discardableResult
  public func setReplication(path: String, factor:ushort) throws {

    // perform the test
    let (_,dat,_) = try perform(operation: "SETREPLICATION", path:path, variables: ["replication": factor], method:.PUT)

    if dat.asBoolean {
      return
    }//end if

    throw Exception.unexpectedReturn
  }//end setReplication

  /// set times
  /// - parameters
  ///   - path: full path of the remote file / directory
  ///   - modification: modification time of the file / directory
  ///   - access: access time of the file / directory
  /// - throws
  /// unauthorized access / Invalid file or directory
  @discardableResult
  public func setTimes(path: String, modification: Int = -1, access: Int = -1) throws{
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

  }//end setTimes

  /// get ACL
  /// - returns:
  /// AclStatus
  /// - parameters
  ///   - path: full path of the remote file / directory
  @discardableResult
  public func getACL(path: String) throws -> AclStatus? {
    // perform a request
    let (_, dat, _) = try perform(operation: "GETACLSTATUS", path: path)
    return dat.asAclStatus
  }//end getACL


  /// set ACL
  /// - parameters
  ///   - path: full path of the remote file / directory
  ///   - specification: ACL spec string, such as "user::rwx,user:user1:---,group::rwx,other::rwx"
  /// - throws
  /// unauthorized access / Invalid file or directory
  @discardableResult
  public func setACL(path: String, specification: String) throws {
    // perform the test
    let _ = try perform(operation: "SETACL", path:path, variables: ["aclspec":specification], method:.PUT)
  }//end setACL

  /// modify ACL entries
  /// - parameters
  ///   - path: full path of the remote file / directory
  ///   - entries: ACL spec string, such as "user::rwx,user:user1:---,group::rwx,other::rwx"
  /// - throws
  /// unauthorized access / Invalid file or directory
  @discardableResult
  public func modifyACL(path: String, entries: String) throws {

    // perform the test
    let _ = try perform(operation: "MODIFYACLENTRIES", path:path, variables: ["aclspec":entries], method:.PUT)
  }//end modifyACL

  /// remove ACL entries
  /// - parameters
  ///   - path: full path of the remote file / directory
  ///   - entries: ACL spec string, such as "user::rwx,user:user1:---,group::rwx,other::rwx"
  /// - throws
  /// unauthorized access / Invalid file or directory
  @discardableResult
  public func removeACL(path: String, entries: String = "", defaultACL:Bool = false) throws {

    if defaultACL {
      let _ = try perform(operation: "REMOVEDEFAULTACL", path:path, method:.PUT)
    } else {
      if entries.isEmpty {
        let _ = try perform(operation: "REMOVEACL", path:path, method:.PUT)
      } else {
        let _ = try perform(operation: "REMOVEACLENTRIES", path:path, variables: ["aclspec": entries], method:.PUT)
      }//end if
    }//end if
  }//end removeACL

  /// check access
  /// - parameters:
  ///   - fsaction: file system action, open / append, etc.
  /// - throws
  /// unexpectedResponse
  /// - returns:
  /// true for access granted, false for access denied
  @discardableResult
  public func checkAccess(path: String, fsaction: String) throws -> Bool {
    let (_,dat,_) = try perform(operation: "CHECKACCESS", path:path, variables:["fscation": fsaction])
    return dat.asBoolean
  }//end checkAccess

  /// set xattr
  /// - parameters:
  ///   - path: full path of the remote file / directory
  ///   - name: xattr name
  ///   - value: xattr value
  ///   - flag: xattr flag, only valid for CREATE OR REPLACE
  /// - throws:
  /// unauthorized access / Invaid file or directory
  @discardableResult
  public func setXAttr(path: String, name: String, value: String, flag: XAttrFlag = .CREATE) throws {

    // perform a request
    let _ = try perform(operation: "SETXATTR", path: path, variables: ["xattr.name": name, "xattr.value": value, "flag": flag.rawValue], method:.PUT)
  }//end setXAttr

  /// remove xattr
  /// - parameters:
  ///   - path: full path of the remote file / directory
  ///   - name: xattr name
  /// - throws:
  /// unauthorized access / Invaid file or directory
  @discardableResult
  public func removeXAttr(path: String, name: String) throws {
    // perform a request
    let _ = try perform(operation: "REMOVEXATTR", path: path, variables: ["xattr.name": name], method:.PUT)
  }//end removeXAttr

  /// get xattr
  /// - returns:
  /// [XAttr]
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
    let (_, dat, _) = try perform(overwriteURL: url)
    return dat.asXAttrs
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
    let (_, dat, _) = try perform(operation: "LISTXATTRS", path:path)

    return dat.asXAttrNames
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
    let (_, dat, _) = try perform(operation: "CREATESNAPSHOT", path:path, method: .PUT)

    // decode the json return
    let shot = dat.asPath

    let array = shot.characters.split(separator: "/").map(String.init)
    if array.isEmpty {
      throw Exception.unexpectedReturn
    }//end if
    let name = array[array.count - 1]
    return (shot, name)
  }//end createSnapshot

  /// delete snapshot
  /// - parameters:
  ///   - path: full path of the remote file / directory
  ///   - name: snapshot name
  /// - throws:
  /// unauthorized access / Invaid file or directory
  @discardableResult
  public func deleteSnapshot(path: String, name: String) throws	{

    // perform a request
    let _ = try perform(operation: "DELETESNAPSHOT", path:path, variables: ["snapshotname": name], method: .DELETE)
  }//end deleteSnapshot

  /// rename snapshot
  /// - parameters:
  ///   - path: full path of the remote file / directory
  ///   - from: snapshot original name
  ///   - to: new name of the snapshot
  /// - throws:
  /// unauthorized access / Invaid file or directory
  @discardableResult
  public func renameSnapshot(path: String, from: String, to: String) throws {

    // perform a request
    let _ = try perform(operation: "RENAMESNAPSHOT", path:path, variables: ["oldsnapshotname": from, "snapshotname":to], method: .PUT) {
      curl in

      curl.setOption(CURLOPT_FOLLOWLOCATION, int: 1)
    }
  }//end renameSnapshot
}//end class
