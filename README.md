# PerfectHadoop WebHDFS [简体中文](README.zh_CN.md)

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
PerfectHadoop supports Hadoop 3.0.0 with a limitation on 2.7.3.

## Building
Add this project as a dependency in your Package.swift file.

``` swift
.Package(url:"https://github.com/PerfectlySoft/Perfect-Hadoop.git", majorVersion: 1, minor: 0)
```

## Quick Start

### Import WebHDFS library
Before using WebHDFS class, import the header first:

``` swift
import PerfectHadoop
```

### Connect to Hadoop

To connect to your HDFS server by WebHDFS, initialize a WebHDFS object with sufficient parameters:

``` swift
// this connection could possibly do some basic operations
let hdfs = WebHDFS(host: "hdfs.somedomain.com", port: 9870)
```
or connect to Hadoop with a valid user name:

``` swift
// add user name to do more operations such as modifiction of file or directory
let hdfs = WebHDFS(host: "hdfs.somedomain.com", port: 9870, user: "username")
```

#### Authentication
If using Kerberos to authenticate, please try codes below:

``` swift
// set auth to kerberos
let hdfs = WebHDFS(host: "hdfs.somedomain.com", port: 9870, user: "username", auth: .krb5)
```


#### Parameters of WebHDFS Object
- `service`:String, the service protocol of web request - http / https / webhdfs / hdfs
- `host`:String, the hostname or ip address of the webhdfs host
- `port`:Int, the port of webhdfs host, default is 9870
- `auth`: Authorization Model, .off or .krb5. Default value is .off
- `proxyUser`:String, proxy user, if applicable
- `extraHeaders`: Closure, extra headers if special header such as CSRF (Cross-Site Request Forgery Prevention) is applicable
- `apibase`:String, use this parameter ONLY the target server has a different api routine other than /webhdfs/v1
- `timeout`:Int, timeout in seconds, zero means never timeout during transfer

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
`getFileStatus()` will return a `FileStatus` structure with properties below:

#### Properties of `FileStatus` Structure
- `accessTime`: Int, unix time for last access
- `pathSuffix`: String, file suffix / extension - type
- `replication`: Int, replicated nodes count
- `type`: String, node type: directory or file
- `blockSize`: Int, storage unit, default = 128M, min = 1M
- `owner`: String, user name of the node owner
- `modificationTime`: Int, last modification in unix epoch time format
- `group`: String, group name of the node
- `permission`: Int, node permission, (u)rwx (g)rwx (o)rwx
- `length`:Int, file length

To get status info from a file or a directory, call `getFileStatus()` as example below:

``` swift
let fs = try hdfs.getFileStatus(path: "/")
if fs.length > 0 {
	...
}
```

### List Status
Method `listStatus()` will return an array of `[FileStatus]`, i.e., a list of all files with status under a specific directory. For example,

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
try hdfs.mkdir(path: "/demo", permission: 754)
```

### Summary of Directory
WebHDFS provides a `getDirectoryContentSummary()` method to developers and  will return detail info as defined below:

#### Properties of `ContentSummary` Structure
- `directoryCount`: Int, how many sub folders does this node have
- `fileCount`: Int, file count of the node
- `length`: Int, length of the node
- `quota`: Int, quota of the node
- `spaceConsumed`: Int, blocks that node consumed
- `spaceQuota`: Int, block quota
- `typeQuota`: Three Quota Structures, with two properties of each: `consumed` and `quota`, both properties are integers:
  - `ARCHIVE`: Quota, quota info about data stored in archived files
  - `DISK`: Quota, quota info about data stored in hard disk
  - `SSD`: Quota, quota info about data stored in SSD

To get this summary, call `getDirectoryContentSummary()` with path info:

``` swift
let sum = try hdfs.getDirectoryContentSummary(path: "/")
print(sum.length)
print(sum.spaceConsumed)
print(sum.typeQuota.SSD.consumed)
print(sum.typeQuota.SSD.quota)
print(sum.typeQuota.DISK.consumed)
print(sum.typeQuota.DISK.quota)
print(sum.typeQuota.ARCHIVE.consumed)
print(sum.typeQuota.ARCHIVE.quota)
...
```

### Checksum

Checksum method `getFileCheckSum()` helps user check integrity of file by three properties of `FileChecksum` Structure:

#### Property of `FileChecksum` Structure
- `algorithm`: String, algorithm information of this checksum
- `bytes`: String, checksum string result
- `length`: Int, length of the string

Here is an example of checksum:

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
To delete a directory or a file, simply call `delete()`. If the object to remove is a directory, users can also apply another parameter of `recursive`. If set to true, the directory will be removed with all sub folders.

``` swift
// remove a file
try hdfs.delete(path "/demo/boo.txt")

// remove a directory, recursively
try hdfs.delete(path:"/demo", recursive: true)
```

### Upload
To upload a file, call `create()` method, with two parameters essentially, i.e., local file to upload and the expected remote file path, as below:

``` swift
try hdfs.create(path: "/destination", localFile: "/tmp/afile.txt")
```
Considering it is a time consuming operation, please consider to call this function in a threading way practically.

#### Parameters
Parameters of `create()` include:

- `path`:String, full path of the remote file / directory.
- `localFile`:String, full path of file to upload
- `overwrite`:Bool, If a file already exists, should it be overwritten?
- `permission`: Int, unix style file permission (u)rwx (g)rwx (o)rwx. Default is 755, i.e., rwxr-xr-x
- `blocksize`:Int, size of per block unit. default 128M, min = 1M
- `replication`:Int, The number of replications of a file.
- `buffersize`: The size of the buffer used in transferring data.

### Symbol Link
The same as Unix system, HDFS provides a method called `createSymLink` to create a symbolic link to another file or directory:

``` swift
try hdfs.createSymLink(path: "/book/longname.txt", destination:"/my/recent/quick.lnk", createParent: true)
```
Please note that there is a parameter called `createParent`, which means if there is no such a path, the system will automatically create a full path as demand, i.e, if there is no such a path of "recent" under folder of "my", then it will be automatically created.

### Download
To download a file, call `openFile()` method as below:

``` swift
let bytes = try hdfs.openFile(path: "/books/bedtimestory.txt")
print(bytes.count)
```
In this example, the content of "bedtimestory.txt" will be save to an binary byte array called `bytes` 

Considering it is a time consuming operation, please consider to call this function in a threading way practically. In this case, please also consider to call `openFile()` for serveral times to get the downloading process, as indicated by the parameters below, which means you can download the file by pieces, and if something wrong, you can also re-download the failure parts:

#### Parameters
- `path`:String, full path of the remote file / directory.
- `offset`:Int, The starting byte position.
- `length`:Int, The number of bytes to be processed.
- `buffersize`:Int, The size of the buffer used in transferring data.


### Append
Append operation is similar to `create`, instead of overwriting, it will append the local file content to the end of the remote file:

``` swift
try hdfs.append(path: "/remoteFile.txt", localFile: "/tmp/b.txt")
```

#### Parameters
- `path`:String, full path of the remote file / directory.
- `localFile`:String, full path of file to upload
- `buffersize`:Int, The size of the buffer used in transferring data.

### Merge Files
HDFS allows user to concat two or more files into one, for example:

``` swift
try hdfs.concat(path:"/tmp/1.txt", sources:["/tmp/2.txt", "/tmp/3.txt"])
```

Then file 2.txt and 3.txt will all append to 1.txt

### Truncate
File on an HDFS could be truncated into expected length as below:

``` swift
try hdfs.truncate(path: "/books/LordOfRings.txt", newlength: 1024)
```
The above example will trim the file into 1k.

### Set Permission
HDFS file permission can be set by method of `setPermission`. The example below demonstrates how to set "/demo" directory with a permission of 754, i.e., rwxr-xr-- (read/write/execute for user, read/execute for group and read only for others):

``` swift
try hdfs.setPermission(path: "/demo", permission: 754)
```

### Set Owner
Ownership of a file or a directory can be transferred by a method called `setOwner`:

``` swift
try hdfs.setOwner(path: "/book/chickenrun.html", name:"NewOwnerName", group: "NewGroupName")
```

### Set Replication
Files on HDFS system can be replicated on more than one node. Use `setReplication` do this job:

``` swift
try hdfs.setReplication(path: "/book/twins.txt", factor: 2)
// if success, twins.txt will have two replications
```

### Access & Modification Time
HDFS accepts changing the access or modification time info of a file. The time is in Epoch / Unix timestamp format. The example below shows a similar operation of unix command `touch`:

``` swift
let now = time(nil)
try hdfs.setTime(path: "/tmp/touchable.txt", modification: now, access: now)
// if success, the time info of the file will be updated.
```

### Access Control List
Access control list of HDFS file system can be operated by the following methods:

- `getACL`: retrieve the ACL info
- `setACL`: set the ACL info
- `modifyACL`: modify the ACL entries
- `removeACL`: remove one or more ACL entries, or remove all entries by default.

The `getACL()` method will return an `AclStatus` structure, with properties below:

- `entries`: [String], an array of ACL entry strings.
- `owner`: String, the user who is the owner
- `group`: String, the group owner
- `permission`: Int, permission code in unix style
- `stickyBit`: Bool, true if the sticky bit is on

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

	try hdfs.setACL(path: remoteFile, specification: "user::rw-,user:hadoop:rw-,group::r--,other::r--")

	try hdfs.modifyACL(path: remoteFile, entries: "user::rwx,user:hadoop:rwx,group::rwx,other::---")

	try hdfs.removeACL(path: remoteFile, defaultACL: false)
	try hdfs.removeACL(path: remoteFile)
	try hdfs.removeACL(path: remoteFile, entries: "", defaultACL: false)
```

### Check Access
Method `checkAccess()` is for checking whether a specific action is accessible or not. Typical Usage of this method is:

``` swift
let b = try hdfs.checkAccess(path: "/", fsaction: "mkdir")
// true value means user can perform mkdir() on the root folder
if b {
	print("mkdir: Access Granted")
} else {
	print("mkdir: Access Denied")
}
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

try hdfs.setXAttr(path: remoteFile, name: "user.color", value: "red")
// if success, an attribute called 'user.color' with a value of 'red' will be added to the file 'a.txt'

try hdfs.setXAttr(path: remoteFile, name: "user.size", value: "small")
// if success, an attribute called 'user.size' with a value of 'small' will be added to the file 'a.txt'

try hdfs.setXAttr(path: remoteFile, name: "user.build", value: "2016")
// if success, an attribute called 'user.build' with a value of '2016' will be added to the file 'a.txt'

try hdfs.setXAttr(path: remoteFile, name: "user.build", value: "2017", flag:.REPLACE)
// please note the flag of REPLACE. if true, an attribute called 'user.build' will be replaced with the new value of 2017 from 2016

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

try hdfs.removeXAttr(path: remoteFile, name: "user.size")
// if success, the attribute of user.size will be removed

```

### Snapshots
HDFS provides snapshots functions for directories. 

- `CreateSnapshot()`

If success, function `createSnapshot()` will return a tuple `(longname, shortname)`. The long name is the full path of the snapshot, and the short name is the snapshot's own name. Check the codes below:

```swift
let (fullpath, shortname) = try hdfs.createSnapshot(path: "/mydata")
print(fullpath)
print(shortname)
```

- `renameSnapshot()`
This function can rename the snapshot from its short name to a new one:

``` swift
try hdfs.renameSnapshot(path: "/mydata", from: shortname, to: "snapshotNewName")
```

- `deleteSnapshot()`

Once having the short name of snapshot, `deleteSnapshot()` can be used to delete the snapshot:

``` swift
try hdfs.deleteSnapshot(path: dir, name: shortname)
```

## Further Information
For more information on the Perfect project, please visit [perfect.org](http://perfect.org).
