# Perfect - WebHDFS Connector [简体中文](README.zh_CN.md)

<p align="center">
    <a href="http://perfect.org/get-involved.html" target="_blank">
        <img src="http://perfect.org/assets/github/perfect_github_2_0_0.jpg" alt="Get Involed with Perfect!" width="854" />
    </a>
</p>

<p align="center">
    <a href="https://github.com/PerfectlySoft/Perfect" target="_blank">
        <img src="http://www.perfect.org/github/Perfect_GH_button_1_Star.jpg" alt="Star Perfect On Github" />
    </a>  
    <a href="http://stackoverflow.com/questions/tagged/perfect" target="_blank">
        <img src="http://www.perfect.org/github/perfect_gh_button_2_SO.jpg" alt="Stack Overflow" />
    </a>  
    <a href="https://twitter.com/perfectlysoft" target="_blank">
        <img src="http://www.perfect.org/github/Perfect_GH_button_3_twit.jpg" alt="Follow Perfect on Twitter" />
    </a>  
    <a href="http://perfect.ly" target="_blank">
        <img src="http://www.perfect.org/github/Perfect_GH_button_4_slack.jpg" alt="Join the Perfect Slack" />
    </a>
</p>

<p align="center">
    <a href="https://developer.apple.com/swift/" target="_blank">
        <img src="https://img.shields.io/badge/Swift-3.0-orange.svg?style=flat" alt="Swift 3.0">
    </a>
    <a href="https://developer.apple.com/swift/" target="_blank">
        <img src="https://img.shields.io/badge/Platforms-OS%20X%20%7C%20Linux%20-lightgray.svg?style=flat" alt="Platforms OS X | Linux">
    </a>
    <a href="http://perfect.org/licensing.html" target="_blank">
        <img src="https://img.shields.io/badge/License-Apache-lightgrey.svg?style=flat" alt="License Apache">
    </a>
    <a href="http://twitter.com/PerfectlySoft" target="_blank">
        <img src="https://img.shields.io/badge/Twitter-@PerfectlySoft-blue.svg?style=flat" alt="PerfectlySoft Twitter">
    </a>
    <a href="http://perfect.ly" target="_blank">
        <img src="http://perfect.ly/badge.svg" alt="Slack Status">
    </a>
</p>



This project provides a Swift wrapper of WebHDFS API, enabling access to Hadoop servers.

This package builds with Swift Package Manager and is part of the [Perfect](https://github.com/PerfectlySoft/Perfect) project. It was written to be stand-alone and so does not require PerfectLib or any other components.

Ensure you have installed and activated the latest Swift 3.0 tool chain.

## Issues

We are transitioning to using JIRA for all bugs and support related issues, therefore the GitHub issues has been disabled.

If you find a mistake, bug, or any other helpful suggestion you'd like to make on the docs please head over to [http://jira.perfect.org:8080/servicedesk/customer/portal/1](http://jira.perfect.org:8080/servicedesk/customer/portal/1) and raise it.

A comprehensive list of open issues can be found at [http://jira.perfect.org:8080/projects/ISS/issues](http://jira.perfect.org:8080/projects/ISS/issues)

## Release Note
PerfectWebHDFS supports Hadoop 3.0.0 with a limitation on 2.7.3.

## Building
Add this project as a dependency in your Package.swift file.

``` swift
.Package(url:"https://github.com/PerfectlySoft/Perfect-WebHDFS.git", majorVersion: 2, minor: 0)
```

## Quick Start

### Import WebHDFS library
Before using WebHDFS class, import the header first:

``` swift
import PerfectWebHDFS
```

### Authentication
Before connecting to a Hadoop host, please make sure the proper authentication of WebHDFS. PerfectWebHDFS accepts three basic authentication model:

``` swift

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
```

So when initializing a WebHDFS object, please choose one of the above model, i.e., .byUser or .byKerberos to pass a valid user name. If ignored, the PerfetWebHDFS will neither negotiate nor send user name to the server.

### Connect to Hadoop

To connect to your HDFS server by WebHDFS, initialize a WebHDFS object with sufficient parameters:

```
let hdfs = WebHDFS(host: "hdfs.somedomain.com", port: 50070, auth: .byUser(name:"hdfsOperator"))
```

#### Parameters of WebHDFS()
- *service*: the service protocol of web request - http / https / webhdfs / hdfs
- *host*: the hostname or ip address of the webhdfs host
- *port*: the port of webhdfs host, default is 9870
- *auth*: Authorization Model, default value is .off
- *proxyUser*: proxy user, if applicable
- *extraHeaders*: extra headers if special header such as CSRF (Cross-Site Request Forgery Prevention) is applicable
- *apibase*: use this parameter ONLY the target server has a different api routine other than /webhdfs/v1
- *timeout*: timeout in seconds, zero means never timeout during transfer

### Exceptions
In case of operation failure, an exception might be thrown out. User can catch it and check the (url, header, body) of the failure, as demo below:

``` swift
let hdfs = WebHDFS()
do {
	let fs = try hdfs.getFileStatus(path: "/")
	// some operations
	...
}
catch(WebHDFS.Exception.unexpectedResponse(let (url, header, body))) {
	print("WebHDFS Exception: \(url)\n\(header)\n\(body)")
}
catch (let err){
	print("Other Error:\(err)")
}
```

### Get Home Directory
Call `getHomeDirectory()` to get the home directory for current user.

``` swift
let home = try hdfs.getHomeDirectory()
print("the home is \(home)")
```

### Get File Status
`getFileStatus()` will return a file status structure as defined below:

``` swift
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

  }//end struct

```

To get status info from a file or a directory, call `getFileStatus()` as example below:

``` swift
let fs = try hdfs.getFileStatus(path: "/")
if fs.length > 0 {
	...
}
```

### List Status
Method `listStatus()` can list all files with status under a specific directory. For example,

``` Swift
let list = try hdfs.listStatus(path: "/")
for file in list {
	// print the ownership of a file in the list
	print(file.owner)
}
```
The structure of item listed is the same with `getFileStatus()`.


### Create Directory
Basic HDFS directory operations include `mkdir` and `delete`. To create a new directory named "/demo" with a permission 754, i.e., rwxr-xr-- (read/write/execute for user, read/execute for group and read only for others), try the line of code below:

``` swift
let res = try hdfs.mkdir(path: "/demo", permission: 754)
// if success, res will be true
print(res)
```

### Summary of Directory
PerfectHDFS provides a `getDirectoryContentSummary()` method to developers and  will return detail info as defined below:

``` swift
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
 ```

To get this summary, call `getDirectoryContentSummary()` with path info:

``` swift
let sum = try hdfs.getDirectoryContentSummary(path: "/")
print(sum.length)
print(sum.spaceConsumed)
...
```

### Checksum
Checksum helps user check integrity of file:

``` swift
let checksum = try hdfs.getFileCheckSum(path: "/book/chickenrun.txt")
// checksum is a struct:
// algorithm information of this checksum
print(checksum.algorithm)
// checksum string
print(checksum.bytes)
// string length
print(checksum.length)
```

### Delete
To delete a directory or a file, simply call `delete()`. If the object to remove is a directory, users can also apply another parameter of `recursive`. If set to ture, the directory will be removed with all sub folders.

``` swift
// remove a file
let _ = try hdfs.delete(path "/demo/boo.txt")

// remove a directory
let res = try hdfs.delete(path:"/demo", recursive: true)
guard res == true else {
	// something wrong
	...
}
```

### Upload
To upload a file, call `create()` method, with two parameters essentially, i.e., local file to upload and the expected remote file path, as below:

``` swift
let res = hdfs.create(path: "/destination", localFile: "/tmp/afile.txt")
/// res will be true if success
```
Considering it is a time consuming operation, please consider to call this function in a threading way practically.

#### Parameters
Parameters of `create()` include:

- path: full path of the remote file / directory.
- localFile: full path of file to upload
- overwrite: If a file already exists, should it be overwritten?
- permission: unix style file permission (u)rwx (g)rwx (o)rwx. Default is 755, i.e., rwxr-xr-x
- blocksize: size of per block unit. default 128M, min = 1M
- replication: The number of replications of a file.
- buffersize: The size of the buffer used in transferring data.

### Symbol Link
The same as Unix system, HDFS provides a method called `createSymLink` to create a symbolic link to another file or directory:

``` swift
let res = try hdfs.createSymLink(path: "/book/longname.txt", destination:"/my/recent/quick.lnk", createParent: true)
// if success, the value of res will be true.
print(res)
```
Please note that there is a parameter called `createParent`, which means if there is no such a path, the system will automatically create a full path as demand, i.e, if there is no such a path of "recent" under folder of "my", then it will be automatically created.

### Download
To download a file, call `openFile()` method as below:

``` swift
let bytes = try hdfs.openFile(path: "/books/bedtimestory.txt")
print(bytes.count)
```
In this example, the content of "bedtimestory.txt" will be save to an binary byte array called `bytes` 

Considering it is a time consuming operation, please consider to call this function in a threading way practically. In this case, please also consider to call `openFile()` for serveral times to get the downlowing process, as indicated by the parameters below, which means you can download the file by pieces, and if something wrong, you can also re-download the failure parts:

#### Parameters
- path: full path of the remote file / directory.
- offset: The starting byte position.
- length: The number of bytes to be processed.
- buffersize: The size of the buffer used in transferring data.


### Append
Append operation is similar to `create`, instead of overwriting, it will append the local file content to the end of the remote file:

``` swift
let res = try hdfs.append(path: "/remoteFile.txt", localFile: "/tmp/b.txt")
/// res will be true if success
```

#### Parameters
- path: full path of the remote file / directory.
- localFile: full path of file to upload
- buffersize: The size of the buffer used in transferring data.

### Merge Files
HDFS allows user to concat two or more files into one, for example:

``` swift
let res = try hdfs.concat(path:"/tmp/1.txt", sources:["/tmp/2.txt", "/tmp/3.txt"])
// return true if success
```

Then file 2.txt and 3.txt will all append to 1.txt

### Truncate
File on an HDFS could be truncated into expected length as below:

``` swift
let res = try hdfs.truncate(path: "/books/LordOfRings.txt", newlength: 1024)
// return true if success
```
The above example will trim the file into 1k.

### Set Permission
HDFS file permission can be set by method of `setPermission`. The example below demonstrates how to set "/demo" directory with a permission of 754, i.e., rwxr-xr-- (read/write/execute for user, read/execute for group and read only for others):

``` swift
let res = try hdfs.setPermission(path: "/demo", permission: 754)
// if success, res will be true
print(res)
```

### Set Owner
Ownership of a file or a directory can be transferred by a method called `setOwner`:

``` swift
let res = try hdfs.setOwner(path: "/book/chickenrun.html", name:"NewOwnerName", group: "NewGroupName")
// if success, res will be true
print(res)
```

### Set Replication
Files on HDFS system can be replicated on more than one node. Use `setReplication` do this job:

``` swift
let res = try hdfs.setReplication(path: "/book/twins.txt", factor: 2)
// if success, res will be true and the twins.txt will have two replications
print(res)
```

### Access & Modification Time
HDFS accepts changing the access or modification time info of a file. The time is in Epoch / Unix timestamp format. The example below shows a similar operation of unix command `touch`:

``` swift
let now = time(nil)
let res = try hdfs.setTime(path: "/tmp/touchable.txt", modification: now, access: now)
// if success, res will be true and the time info of the file will be updated.
print(res)
```

### Access Control List
Access control list of HDFS file system can be operated by the following methods:

- `getACL`: retrieve the ACL info
- `setACL`: set the ACL info
- `modifyACL`: modify the ACL entries
- `removeACL`: remove one or more ACL entries, or remove all entries by default.

The `getACL()` method will return an `AclStatus` structure, as defined below:

``` swift
  /// ACL status class
  public class AclStatus: JSONStruct {
    // ACL entries
    var entries: [String] = []
    var group: String = ""
    var owner: String = ""
    var permission: Int = 775
    var stickyBit: Bool = false
  }//end class
```

The following example demonstrates all basic ACL operations:  

``` swift
let hdfs = WebHDFS(auth:.byUser(name: defaultUserName))
let remoteFile = "/acl.txt"
do {
	// get access control list
	var acl = try hdfs.getACL(path: remoteFile)
	print("group info: \(acl.group)")
	print("owner info: \(acl.owner)")
	print("entry info: \(acl.entries)")
	print("permission info: \(acl.permission)")
	print("stickyBit info: \(acl.stickyBit)")

	var res = try hdfs.setACL(path: remoteFile, specification: "user::rw-,user:hadoop:rw-,group::r--,other::r--")

	print("Setting res: \(res)")

	res = try hdfs.modifyACL(path: remoteFile, entries: "user::rwx,user:hadoop:rwx,group::rwx,other::---")

	print("Modifying res: \(res)")

	let _ = try hdfs.removeACL(path: remoteFile, defaultACL: false)
	let _ = try hdfs.removeACL(path: remoteFile)
	let _ = try hdfs.removeACL(path: remoteFile, entries: "", defaultACL: false)
```
### Check Access
Method `checkAccess()` is for checking whether a specific action is accessible or not. Typical Usage of this method is:

``` swift
let res = try hdfs.checkAccess(path: "/", fsaction: "mkdir")
// true value means user can perform mkdir() on the root folder
print(res)
```

### Extension Attributes
Besides the traditional file attributes, HDFS also provides an extension method for more customerizable attributes, which is named as XAttr. XAttr operations include:

- `setXAttr`: set the attributes
- `getXAttr`: get one or more attributes' value
- `listXAttr`: list all attributes
- `removeXAttr`: remove one or more attributes.

Besides, there are two flags for XAttr opertions: `CREATE` and `REPLACE`. The default flag is CREATE when setting an XAttr.
  
``` swift 
public enum XAttrFlag:String {
	case CREATE = "CREATE"
	case REPLACE = "REPLACE"
}
```
Please check the code below:

``` swift
let remoteFile = "/book/a.txt"

var res = try hdfs.setXAttr(path: remoteFile, name: "user.color", value: "red")
// if true, an attribute called 'user.color' with a value of 'red' will be added to the file 'a.txt'
print(res)

res = try hdfs.setXAttr(path: remoteFile, name: "user.size", value: "small")
// if true, an attribute called 'user.size' with a value of 'small' will be added to the file 'a.txt'
print(res)

res = try hdfs.setXAttr(path: remoteFile, name: "user.build", value: "2016")
// if true, an attribute called 'user.build' with a value of '2016' will be added to the file 'a.txt'
print(res)

res = try hdfs.setXAttr(path: remoteFile, name: "user.build", value: "2017", flag:.REPLACE)
// please note the flag of REPLACE. if true, an attribute called 'user.build' will be replaced with the new value of 2017 from 2016
print(res)

// list all attributes
let list = try hdfs.listXAttr(path: remoteFile)
list.forEach {
	item in
	print(item)
}//next

// retrieve specific attributes      
var a = try hdfs.getXAttr(path: remoteFile, name: ["user.color", "user.size", "user.build"])
// print the attributes with value
a.forEach{
	x in
	print("\(x.name) => \(x.value)")
}//next

res = try hdfs.removeXAttr(path: remoteFile, name: "user.size")
// if true, the attribute of user.size will be removed
print(res)

```

### Snapshots
HDFS provides snapshots functions for directories. Methods of `createSnapshot()`, `renameSnapshot()` and `deleteSnapshot` would be available if essential permissions were granted, i.e., administrator must perform a command as below to allow snapshot operation of a directory called "/mydata":

``` bash
hdfs dfsadmin -allowSnapshot /mydata
```

- `CreateSnapshot()`

If success, function `createSnapshot()` will return a turple `(longname, shortname)`. The long name is the full path of the snapshot, and the short name is the snapshot's own name. Check the codes below:

```swift
let (fullpath, shortname) = try hdfs.createSnapshot(path: "/mydata")
print(fullpath)
print(shortname)
```

- `renameSnapshot()`
This function can rename the snapshot from its short name to a new one:

``` swift
let res = try hdfs.renameSnapshot(path: "/mydata", from: shortname, to: "snapshotNewName")
// true for success
print(res)
```

- `deleteSnapshot()`

Once having the short name of snapshot, `deleteSnapshot()` can be used to delete the snapshot:

``` swift
let res = try hdfs.deleteSnapshot(path: dir, name: shortname)
// true for success
print(res)
```

## Further Information
For more information on the Perfect project, please visit [perfect.org](http://perfect.org).
