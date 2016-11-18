# Perfect - WebHDFS Connector [English](README.md)

<p align="center">
    <a href="http://perfect.org/get-involved.html" target="_blank">
        <img src="http://perfect.org/assets/github/perfect_github_2_0_0.jpg" alt="Get Involed with Perfect!" width="854" />
    </a>
</p>

<p align="center">
    <a href="https://github.com/PerfectlySoft/Perfect" target="_blank">
        <img src="http://www.perfect.org/github/Perfect_GH_button_1_Star.jpg" alt="Star Perfect On Github" />
    </a>  
    <a href="https://gitter.im/PerfectlySoft/Perfect" target="_blank">
        <img src="http://www.perfect.org/github/Perfect_GH_button_2_Git.jpg" alt="Chat on Gitter" />
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
    <a href="https://gitter.im/PerfectlySoft/Perfect?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge" target="_blank">
        <img src="https://img.shields.io/badge/Gitter-Join%20Chat-brightgreen.svg" alt="Join the chat at https://gitter.im/PerfectlySoft/Perfect">
    </a>
    <a href="http://perfect.ly" target="_blank">
        <img src="http://perfect.ly/badge.svg" alt="Slack Status">
    </a>
</p>


该项目实现了一个对 WebHDFS 网络接口的封装，用于访问 Hadoop 服务器。

该软件使用SPM进行编译和测试，本软件也是[Perfect](https://github.com/PerfectlySoft/Perfect)项目的一部分。本软件包可独立使用，因此使用时可以脱离PerfectLib等其他组件。

请确保您已经安装并激活了最新版本的 Swift 3.0 tool chain 工具链。

### 问题报告、内容贡献和客户支持

我们目前正在过渡到使用JIRA来处理所有源代码资源合并申请、修复漏洞以及其它有关问题。因此，GitHub 的“issues”问题报告功能已经被禁用了。

如果您发现了问题，或者希望为改进本文提供意见和建议，[请在这里指出](http://jira.perfect.org:8080/servicedesk/customer/portal/1).

在您开始之前，请参阅[目前待解决的问题清单](http://jira.perfect.org:8080/projects/ISS/issues).

## 版本兼容性
PerfectWebHDFS 目前支持 Hadoop 3.0.0，以及 2.7.3 的部分功能。

## 编译
请在您的 Package.swift 文件中增加以下内容

``` swift
.Package(url:"https://github.com/PerfectlySoft/Perfect-WebHDFS.git", majorVersion: 2, minor: 0)
```

## 快速上手

### 导入 WebHDFS 函数库
在使用 WebHDFS 类功能之前，请在您的源代码开头增加以下内容

``` swift
import PerfectWebHDFS
```

### 认证模式
Hadoop 服务器有若干种不同的认证模式用于验证用户身份。目前 WebHDFS 函数库支持以下三种模式的访问方法：

``` swift

  /// HDFS 用户身份验证模式
  public enum Authentication {
    
    /// 默认为无身份验证模式 —— 甚至没有用户名
    case off

    /// 伪身份验证模式 - 只需要用户名
    case byUser(name: String)
    
    /// Kerberos 验证模式 - 用户名用于执行权限操作，但请注意本函数库不负责身份验证，用户登录获取或注销凭证的过程需要客户通过 kinit / kdestroy 命令自行完成！
    case byKerberos(name: String)
  }// end Authentication
```

因此，在使用 WebHDFS 对象时，请务根据实际情况选择合适的模式。比如，如果要进行文件系统写操作，则必须使用 .byUser or .byKerberos 并传递一个用户名给函数库。默认情况下函数库不会给服务器发用户名，也不会进行身份验证协商。
 
### 连接到 Hadoop 集群

如果希望通过 webhdfs 连接到 HDFS 集群，请初始化一个 WebHDFS 类对象并包含必要参数：

```
let hdfs = WebHDFS(host: "hdfs.somedomain.com", port: 50070, auth: .byUser(name:"hdfsOperator"))
```

#### WebHDFS() 参数说明
- *service*: 协议字符串，比如 - http / https / webhdfs / hdfs
- *host*: 主机名或 IP 地址
- *port*: webhdfs 主机端口，默认为 9870
- *auth*: 认证模式，默认是 `.off`
- *proxyUser*: 代理用户，如果需要的话
- *extraHeaders*: HTTP 额外的头数据。如果需要避免跨域名间接访问，可能需要设置这个内容（CSRF）。
- *apibase*: 如果目标服务器用的不是标准 /webhdfs/v1/ 路径协议，则在此设置。
- *timeout*: 超时设置。默认情况下所有WebHDFS操作都会阻塞当前进程，因此设置该参数允许秒级等待，一旦超时则解除阻塞立即返回。

### 异常处理
大部分WebHDFS操作中，如果出现异常，请参考下列代码进行错误分析。出现异常时，用户可以获得出错的url地址、返回的消息头和详细数据体：

``` swift
let hdfs = WebHDFS()
do {
	let fs = try hdfs.getFileStatus(path: "/")
	// 执行其他操作
	...
}
catch(WebHDFS.Exception.unexpectedResponse(let (url, header, body))) {
	print("WebHDFS 发现异常：\(url)\n\(header)\n\(body)")
}
catch (let err){
	print("其他错误：\(err)")
}
```
### 获取根目录
调用函数 `getHomeDirectory()` 以返回用户根目录：

``` swift
let home = try hdfs.getHomeDirectory()
print("用户根目录是： \(home)")
```

### 获取文件状态
调用`getFileStatus()`方法可以返回一个如下结构的文件状态表：

``` swift
/// 文件状态表
  public class HdfsFileStatus : JSONStruct {
    
    /// 文件的后缀
    var pathSuffix: String = ""
    
    /// 存储块尺寸，标准为 128MB，最小为 1MB
    var blockSize: ULONG = 0
    
    /// 冗余存储节点数量
    var replication: Int = 0
    
    /// 节点类型：目录还是文件
    var type: String = ""
    
    /// 最近访问时间（unix时间戳)
    var accessTime: ULONG = 0
    
    /// 子目录数量 - 只适用于目录
    var childrenNum: Int = 0
    
    /// hdfs 存储安全规范
    var storagePolicy: Int = 0
    
    /// 每个节点都有一个唯一的文件编码
    var fileId: Int = 0
    
    /// 节点宿主
    var owner: String = ""
    
    /// 最近修改时间（unix时间戳）
    var modificationTime: ULONG = 0
    
    /// 节点用户群组信息
    var group: String = ""
    
    /// 节点权限，标准的unix权限格式： (u)rwx (g)rwx (o)rwx
    var permission: Int = 0
    
    /// 文件长度
    var length: ULONG = 0
    
  }//end struct
  
```

以下代码演示了如何获取文件状态`getFileStatus()`：

``` swift
let fs = try hdfs.getFileStatus(path: "/")
if fs.length > 0 {
	...
}
```

### 文件列表
函数 `listStatus()` 用于列出目录下所有文件及其属性：

``` Swift
let list = try hdfs.listStatus(path: "/")
for file in list {
	// 打印每个文件的宿主用户
	print(file.owner)
}
```
每个文件的状态结构与 `getFileStatus()` 返回的结构相同。


### 创建目录
基本 HDFS 目录操作包括`mkdir()` 和 `delete()`。为了创建一个名为"/demo"的目录，并设置其初始权限为 754（ rwxr-xr-- ，也就是用户可以读写执行、组用户可读可执行、其他用户只读），请参考以下代码:

``` swift
let res = try hdfs.mkdir(path: "/demo", permission: 754)
// 如果成功则结果为真
print(res)
```

### 目录统计
PerfectHDFS 提供目录统计功能 `getDirectoryContentSummary()`，展开后的统计项明细如下：

``` swift 
public class DirectoryContentSummary : JSONStruct {
    
    // 子目录数量
    var directoryCount: Int = 0
    
    // 文件总数
    var fileCount: Int = 0
    
    // 长度
    var length: ULONG = 0
    
    // 节点限额
    var quota: Int  = -1
    
    // 已占用文件块数量
    var spaceConsumed: ULONG = 0
    
    // 文件块限额
    var spaceQuota: Int = -1
    
    // 其他限额
    var typeQuota: [String:[String:Any]] = [:]
  }//end class
 ```

调用 `getDirectoryContentSummary()` 时请输入路径信息，举例如下：

``` swift
let sum = try hdfs.getDirectoryContentSummary(path: "/")
print(sum.length)
print(sum.spaceConsumed)
...
```

### 文件校验
参考下面的程序检查文件的完整性：

``` swift
let checksum = try hdfs.getFileCheckSum(path: "/book/chickenrun.txt")
// 文件累加和算法
print(checksum.algorithm)
// 累加和字符串
print(checksum.bytes)
// 累加和长度
print(checksum.length)
```

### 删除文件或目录
下面的程序展示了如何使用`delete()`函数删除目录或文件:

``` swift
// 删除文件
let _ = try hdfs.delete(path "/demo/boo.txt")

// 删除目录
let res = try hdfs.delete(path:"/demo")
guard res == true else {
	// 如果res不等于逻辑真值，则出错
	...
}
```

### 上传文件
上传文件请调用 `create()` 方法，需要至少包括本地文件名和远程的目标目录名称，比如：

``` swift
let res = hdfs.create(path: "/目标目录", localFile: "/tmp/本地文件.txt")
/// 如果成功则返回为真
print(res)
```
考虑到上传文件是一个耗时的操作，请自行结合超时设置和线程进行上传操作。

#### 参数
方法`create()`的参数包括：

- path: 远程文件的完整路径
- localFile: 本地文件的完整路径
- overwrite: 如果远程文件已存在，是否覆盖
- permission: unix风格文件属性 (u)rwx (g)rwx (o)rwx。默认为755 —— rwxr-xr-x
- blocksize: 每个文件块的尺寸。缺省为 128M，最小值为 1M
- replication: 冗余备份的节点数量。忽略该参数系统将按照 hdfs 默认值进行设置
- buffersize: 传输文件时的缓冲区大小。忽略该参数系统将按照 hdfs 默认值进行设置

### 符号链接
同Unix系统一样，HDFS 提供一个方法叫做`createSymLink`，用于为目录或者文件创建一个符号链接：

``` swift
let res = try hdfs.createSymLink(path: "/book/真文件.txt", destination:"/我的/最近的项目/链接.lnk", createParent: true)
// 如果成功则返回真
print(res)
```
请⚠️注意⚠️其中有一个参数叫做`createParent`，意思是在创建过程中如果没有响应的路径，则系统会自动把对应路径一并创建。

### 文件下载
请使用 `openFile()` 方法下载文件内容

``` swift
let (text, bytes) = try hdfs.openFile(path: "/书刊/睡前故事.txt")
print(bytes.count)
```
上述例子中，文件"睡前故事.txt"会被同时保存到二进制数组 `bytes`中。

因为下载文件通常是一个耗时的操作，请考虑采用多线程异步的方式进行调用。在处理大文件下载时，您还可以针对同一个文件多次调用 `openFile()`方法，实现大文件分段下载。这种方式可以保证在某个文件片段除错后可以进行二次下载，确保文件整体下载内容无误。

#### Parameters
- path: 远程文件完整路径
- offset: 读取文件的起始位置偏移量
- length: 希望读取的字节数
- buffersize: 传输用的缓冲区大小

### 文件追加
文件追加的操作与创建差不多，但不是覆盖，而是将本地文件上传后追加到原文件结尾：

``` swift
let res = try hdfs.append(path: "/远程文件", localFile: "/tmp/本地文件.txt")
// 如果成功则返回真
```
#### Parameters
- path: 远程文件完整路径
- localFile: 本地文件：待上传的本地文件
- buffersize: 文件传输用缓冲区大小
### 文件合并
HDFS 允许用户将多个文件合并到一起：

``` swift
let res = try hdfs.concat(path:"/tmp/1.txt", sources:["/tmp/2.txt", "/tmp/3.txt"])
// 如果成功则返回真
```

上面的例子里，文件 2.txt 和 3.txt 会按顺序追加到 1.txt 之后。

### 截取文件
HDFS 文件可以通过下列函数进行截短：

``` swift
let res = try hdfs.truncate(path: "/书刊/西游记.txt", newlength: 1024)
// 如果成功则返回真
```
如果操作成功，则文件的新长度只剩下 1K。

### 设置权限

HDFS 文件权限的设置方法是`setPermission()`. 以下例子将目录“/demo”的权限设置为了 754， 也就是754（ rwxr-xr-- ，也就是用户可以读写执行、组用户可读可执行、其他用户只读）：

``` swift
let res = try hdfs.setPermission(path: "/demo", permission: 754)
// 如果成功则返回真
print(res)
```

### 设置宿主用户
`setOwner()` 用于将文件宿主设置为其他用户：

``` swift
let res = try hdfs.setOwner(path: "/书刊/小鸡快跑.html", name:"新读者", group: "新群组")
// 如果成功返回真
print(res)
```

### 设置冗余备份数量
HDFS 文件系统允许为每个文件设置一个以上的冗余备份。请使用函数`setReplication()`实现该工作：

``` swift
let res = try hdfs.setReplication(path: "/书刊/侠客行.txt", factor: 2)
// 如果设置成功，res会变成逻辑真值，而《侠客行》会有两个冗余备份
print(res)
```

### 设置访问时间和修改时间
HDFS 允许改变文件的最后访问时间或最近修改时间，格式为unix时间戳。参考以下例子实现类似 unix 的 touch 命令操作：

``` swift
// 取得当前 unix 时间戳
let now = time(nil)
let res = try hdfs.setTime(path: "/tmp/某个文件.txt", modification: now, access: now)
// 如果返回为真则文件的最近访问时间和最近修改时间都会被设置为当前时间
print(res)
```

### 访问控制列表
HDFS 文件系统访问控制列表可以通过以下函数进行修改：

- `getACL`: 获取访问控制列表
- `setACL`: 设置访问控制列表
- `modifyACL`: 修改访问控制列表
- `removeACL`: 删除一个或多个访问列表

`getACL()` 方法会返回一个 `AclStatus`结构，内容如下：

``` swift 
  /// ACL status class
  public class AclStatus: JSONStruct {
    // ACL 清单
    var entries: [String] = []
    // 所在组信息
    var group: String = ""
    // 所在用户信息
    var owner: String = ""
    // 权限
    var permission: Int = 775
    // 标记
    var stickyBit: Bool = false
  }//end class
```

以下程序展示了如何进行访问控制列表操作：

``` swift
let hdfs = WebHDFS(auth:.byUser(name: defaultUserName))
let remoteFile = "/acl.txt"
do {
	// 获取访问控制表
	var acl = try hdfs.getACL(path: remoteFile)
	print("所在组信息：\(acl.group)")
	print("宿主用户信息：\(acl.owner)")
	print("控制清单：\(acl.entries)")
	print("访问权限：\(acl.permission)")
	print("标记：\(acl.stickyBit)")

	var res = try hdfs.setACL(path: remoteFile, specification: "user::rw-,user:hadoop:rw-,group::r--,other::r--")
      
	print("设置结果：\(res)")
      
	res = try hdfs.modifyACL(path: remoteFile, entries: "user::rwx,user:hadoop:rwx,group::rwx,other::---")
   
	print("修改结果：\(res)")
      
	let _ = try hdfs.removeACL(path: remoteFile, defaultACL: false)
	let _ = try hdfs.removeACL(path: remoteFile)
	let _ = try hdfs.removeACL(path: remoteFile, entries: "", defaultACL: false)
```

### 检查命令权限
函数`checkAccess()`用于检验当前用户是否具备某个命令的执行权限：

``` swift
let res = try hdfs.checkAccess(path: "/", fsaction: "mkdir")
// 如果返回为真，则表明当前用户有权在根目录下建立新目录
print(res)
```

### 扩展属性
除了传统的文件属性之外，HDFS 还能够提供可自行定制的更多文件属性，称为扩展属性XAttr，操作包括：

- `setXAttr`: 设置扩展属性
- `getXAttr`: 获得一个或多个扩展属性
- `listXAttr`: 列出所有属性
- `removeXAttr`: 删除一个或多个属性

此外，XAttr 扩展属性还包括两个标志：`CREATE`（创建属性） and `REPLACE`（覆盖数值），在设置扩展属性时默认标志为 CREATE，详见如下定义：
  
``` swift 
public enum XAttrFlag:String {
	case CREATE = "CREATE"
	case REPLACE = "REPLACE"
}
```

请参考以下代码

``` swift
let remoteFile = "/book/a.txt"

var res = try hdfs.setXAttr(path: remoteFile, name: "user.color", value: "red")
// 如果返回真，则文件将增加新属性'user.color'，值为红色
print(res)

res = try hdfs.setXAttr(path: remoteFile, name: "user.size", value: "small")
// 如果返回为真，则文件将增加新属性'user.size'，值为小
print(res)
      
res = try hdfs.setXAttr(path: remoteFile, name: "user.build", value: "2016")
// 如果返回为真，则文件将增加新属性'user.build'，值为2016
print(res)

res = try hdfs.setXAttr(path: remoteFile, name: "user.build", value: "2017", flag:.REPLACE)
// 注意参数标识flag，这里用了REPLACE，意味着扩展属性user.build的值将被替换为2017
print(res)
      
// 打印所有的扩展属性
let list = try hdfs.listXAttr(path: remoteFile)
list.forEach {
	item in
	print(item)
}//next

// 获取特定的扩展属性    
var a = try hdfs.getXAttr(path: remoteFile, name: ["user.color", "user.size", "user.build"])
// 打印获取到的属性
a.forEach{
	x in
	print("\(x.name) => \(x.value)")
}//next

res = try hdfs.removeXAttr(path: remoteFile, name: "user.size")
// 如果为真，则属性中的user.size 将被删除
print(res)

```

### 快照
HDFS 提供目录快照功能，如`createSnapshot()`、`renameSnapshot()` 和 `deleteSnapshot`。快照操作不同于上述操作，需要管理员授权才能执行。比如，如果希望设置 "/mydata"目录下实现快照功能，必须由管理员在主机上进行操作:

``` bash
hdfs dfsadmin -allowSnapshot /mydata
```

- `CreateSnapshot()`

如果成功，函数`createSnapshot()` 将返回一个组合结果`(长名称, 短名称)`。长名称就是路径全称，短名称为快照自己的名字，参考如下命令：

```swift
let (fullpath, shortname) = try hdfs.createSnapshot(path: "/mydata")
// 路径全称
print(fullpath)
// 快照名称
print(shortname)
```

- `renameSnapshot()`
快照重命名：

``` swift
let res = try hdfs.renameSnapshot(path: "/mydata", from: shortname, to: "snapshotNewName")
// 如果为真则快照名称被改变为snapshotNewName
print(res)
```

- `deleteSnapshot()`

用 `deleteSnapshot()` 删除快照：

``` swift
let res = try hdfs.deleteSnapshot(path: dir, name: shortname)
// 如果返回为真，则表示快照被删除
print(res)
```


## 更多信息
关于本项目更多内容，请参考[perfect.org](http://perfect.org).
