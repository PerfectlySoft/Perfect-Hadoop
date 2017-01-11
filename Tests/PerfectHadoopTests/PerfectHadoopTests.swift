import XCTest
import PerfectLib
@testable import PerfectHadoop

class PerfectHadoopTests: XCTestCase {

  let defaultUserName:String = "rockywei"

  func testGetFileStatus() {
    let op = "testGetFileStatus"

    let hdfs = WebHDFS()
    do {
      guard let fs = try hdfs.getFileStatus(path: "/") else {
        XCTFail("file status is null")
        return
      }
      XCTAssertEqual(fs.length, 0)
      XCTAssertEqual(fs.permission, 755)
    }
    catch(WebHDFS.Exception.unexpectedResponse(let (url, header, body))) {
      XCTFail("\(op): \(url)\n\(header)\n\(body)")
    }
    catch (let err){
      XCTFail("\(op):\(err)")
    }
  }

  func testDirOp() {
    var op = "mkdir"
    let hdfs = WebHDFS(user:defaultUserName)
    let dir = "/demo"
    do {
      try hdfs.mkdir(path: dir, permission: 640)
      guard let fs = try hdfs.getFileStatus(path: dir) else {
        XCTFail("file status is null")
        return
      }
      XCTAssertEqual(fs.permission, 640)
    }
    catch(WebHDFS.Exception.unexpectedResponse(let (url, header, body))) {
      XCTFail("\(op): \(url)\n\(header)\n\(body)")
    }
    catch (let err){
      XCTFail("\(op):\(err)")
    }
    op = "rmdir"
    do {
      try hdfs.delete(path: dir)
    }
    catch(WebHDFS.Exception.unexpectedResponse(let (url, header, body))) {
      XCTFail("\(op): \(url)\n\(header)\n\(body)")
    }
    catch (let err){
      XCTFail("\(op):\(err)")
    }
  }

  func testFileCreateOpenDelete() {
    let op = "testFileCreateOpenDelete"
    let hdfs = WebHDFS(user:defaultUserName)
    let remoteFile = "/a.txt"
    let localFilePath = "/tmp/a.txt"
    let localFile = File(localFilePath)
    do {
      try localFile.open(.write)
      try localFile.write(string: "hello")
      localFile.close()
      try hdfs.create(path: remoteFile, localFile: localFilePath, overwrite: true)
      let bytes = try hdfs.openFile(path: remoteFile)
      XCTAssertGreaterThanOrEqual(bytes.count, 5)
      try hdfs.delete(path: remoteFile)
    }
    catch(WebHDFS.Exception.unexpectedResponse(let (url, header, body))) {
      XCTFail("\(op): \(url)\n\(header)\n\(body)")
    }
    catch (let err){
      XCTFail("\(op):\(err)")
    }
  }

  func testFileAppend() {
    let op = "testFileAppend"
    let hdfs = WebHDFS(user:defaultUserName)
    let remoteFile = "/b.txt"
    let localFilePath = "/tmp/b.txt"
    let localFile = File(localFilePath)
    do {
      try localFile.open(.write)
      try localFile.write(string: "had0p")
      localFile.close()
      try hdfs.create(path: remoteFile, localFile: localFilePath, overwrite: true)
      try hdfs.append(path: remoteFile, localFile: localFilePath)
      let bytes = try hdfs.openFile(path: remoteFile)
      XCTAssertGreaterThanOrEqual(bytes.count, 10)
    }
    catch(WebHDFS.Exception.unexpectedResponse(let (url, header, body))) {
      XCTFail("\(op): \(url)\n\(header)\n\(body)")
    }
    catch (let err){
      XCTFail("\(op):\(err)")
    }
  }

  func testFileConcat () {
    let op = "testFileConcat"
    let hdfs = WebHDFS(user:defaultUserName)
    let localFilePath = "/tmp/c.txt"
    let files = ["/1.txt", "/2.txt"]
    let remoteFile = "/0.txt"
    let localFile = File(localFilePath)
    do {
      try localFile.open(.write)
      try localFile.write(string: "12345")
      localFile.close()
      try hdfs.create(path: files[0], localFile: localFilePath, overwrite: true)
      try hdfs.create(path: files[1], localFile: localFilePath, overwrite: true)
      try hdfs.create(path: remoteFile, localFile: localFilePath, overwrite: true)
      try hdfs.concat(path: remoteFile, sources: files)
      let bytes = try hdfs.openFile(path: remoteFile)
      XCTAssertGreaterThan(bytes.count, 14)
      let _ = try hdfs.delete(path: remoteFile)
    }
    catch(WebHDFS.Exception.unexpectedResponse(let (url, header, body))) {
      XCTFail("\(op): \(url)\n\(header)\n\(body)")
    }
    catch (let err){
      XCTFail("\(op):\(err)")
    }
  }

  func testTruncate() {
    let op = "testTruncate"
    let hdfs = WebHDFS(user:defaultUserName)
    let remoteFile = "/d.txt"
    let localFilePath = "/tmp/d.txt"
    let localFile = File(localFilePath)
    do {
      try localFile.open(.write)
      try localFile.write(string: "1234567890123456789012345678901234567890123")
      localFile.close()
      try hdfs.create(path: remoteFile, localFile: localFilePath, overwrite: true)
      try hdfs.truncate(path: remoteFile, newlength:5)
      let bytes = try hdfs.openFile(path: remoteFile)
      XCTAssertGreaterThan(bytes.count, 0)
    }
    catch(WebHDFS.Exception.unexpectedResponse(let (url, header, body))) {
      XCTFail("\(op): \(url)\n\(header)\n\(body)")
    }
    catch (let err){
      XCTFail("\(op):\(err)")
    }
  }

  func testListStatus () {
    let op = "testListStatus"
    let hdfs = WebHDFS(user:defaultUserName)
    do {
      let list = try hdfs.listStatus(path: "/")
      for file in list {
        let name = file.owner
        XCTAssertGreaterThan(name.utf8.count, 0)
      }
    }
    catch(WebHDFS.Exception.unexpectedResponse(let (url, header, body))) {
      XCTFail("\(op): \(url)\n\(header)\n\(body)")
    }
    catch (let err){
      XCTFail("\(op):\(err)")
    }
  }

  func testDirectoryContentSummary () {
    let op = "testDirectoryContentSummary"
    let hdfs = WebHDFS(user:defaultUserName)
    do {
      let list = try hdfs.getDirectoryContentSummary(path: "/")
      XCTAssertGreaterThan(list!.fileCount, 0)
    }
    catch(WebHDFS.Exception.unexpectedResponse(let (url, header, body))) {
      XCTFail("\(op): \(url)\n\(header)\n\(body)")
    }
    catch (let err){
      XCTFail("\(op):\(err)")
    }
  }

  func testFileCheckSum () {
    let op = "testFileCheckSum"
    let hdfs = WebHDFS(user:defaultUserName)
    let remoteFile = "/checksum.txt"
    let localFilePath = "/tmp/checksum.txt"
    let localFile = File(localFilePath)
    do {
      try localFile.open(.write)
      try localFile.write(string: "000000000000000")
      localFile.close()
      try hdfs.create(path: remoteFile, localFile: localFilePath, overwrite: true)
      guard let checksum = try hdfs.getFileCheckSum(path: remoteFile) else {
        XCTFail("checksum is null")
        return
      }
      print(checksum.algorithm)
      print(checksum.bytes)
      print(checksum.length)
      XCTAssertGreaterThan(checksum.length, 0)
    }
    catch(WebHDFS.Exception.unexpectedResponse(let (url, header, body))) {
      XCTFail("\(op): \(url)\n\(header)\n\(body)")
    }
    catch (let err){
      XCTFail("\(op):\(err)")
    }
  }

  func testHomeDirectory () {
    let op = "testHomeDirectory"
    let hdfs = WebHDFS()
    do {
      let home = try hdfs.getHomeDirectory()
      print("the home is ====================> \(home)")
      XCTAssertGreaterThan(home.utf8.count, 0)
    }
    catch(WebHDFS.Exception.unexpectedResponse(let (url, header, body))) {
      XCTFail("\(op): \(url)\n\(header)\n\(body)")
    }
    catch (let err){
      XCTFail("\(op):\(err)")
    }
  }

  func testSettings () {
    let op = "testSettings"
    let hdfs = WebHDFS(user:defaultUserName)
    let remoteFile = "/settings.txt"
    let localFilePath = "/tmp/settings.txt"
    let localFile = File(localFilePath)
    do {
      try localFile.open(.write)
      try localFile.write(string: "configuration")
      localFile.close()
      try hdfs.create(path: remoteFile, localFile: localFilePath, overwrite: true)
      try hdfs.setPermission(path: remoteFile, permission: 640)
      try hdfs.setOwner(path: remoteFile, name: defaultUserName, group: defaultUserName)
      try hdfs.setReplication(path: remoteFile, factor: 2)
      try hdfs.setTimes(path: remoteFile, modification: 1478000000)
      try hdfs.setTimes(path: remoteFile, access: 1478000000)
    }
    catch(WebHDFS.Exception.unexpectedResponse(let (url, header, body))) {
      XCTFail("\(op): \(url)\n\(header)\n\(body)")
    }
    catch (let err){
      XCTFail("\(op):\(err)")
    }
  }
  // to enable acl test, admin must enable ACL in hdfs-site.xml
  /*
   <property>
   <name>dfs.namenode.acls.enabled</name>
   <value>true</value>
   </property>
   */
  func testACL() {
    let op = "testACL"
    let hdfs = WebHDFS(user:defaultUserName)
    let remoteFile = "/acl.txt"
    let localFilePath = "/tmp/acl.txt"
    let localFile = File(localFilePath)
    do {
      try localFile.open(.write)
      try localFile.write(string: "acl test file")
      localFile.close()
      try hdfs.create(path: remoteFile, localFile: localFilePath, overwrite: true)

      guard let acl0 = try hdfs.getACL(path: remoteFile) else {
        XCTFail("acl is null")
        return
      }
      print(acl0.group)
      print(acl0.owner)
      print(acl0.entries)
      print(acl0.permission)
      print(acl0.stickyBit)
      XCTAssertGreaterThan(acl0.permission, 0)

      try hdfs.setACL(path: remoteFile, specification: "user::rw-,user:hadoop:rw-,group::r--,other::r--")

      guard let acl1 = try hdfs.getACL(path: remoteFile) else {
        XCTFail("acl is null")
        return
      }
      print(acl1.group)
      print(acl1.owner)
      print(acl1.entries)
      print(acl1.permission)
      print(acl1.stickyBit)
      XCTAssertGreaterThan(acl1.permission, 0)

      try hdfs.modifyACL(path: remoteFile, entries: "user::rwx,user:hadoop:rwx,group::rwx,other::---")

      guard let acl2 = try hdfs.getACL(path: remoteFile) else {
        XCTFail("acl is null")
        return
      }
      print(acl2.group)
      print(acl2.owner)
      print(acl2.entries)
      print(acl2.permission)
      print(acl2.stickyBit)
      XCTAssertGreaterThan(acl2.permission, 0)

      try hdfs.removeACL(path: remoteFile, defaultACL: false)


      try hdfs.setACL(path: remoteFile, specification: "user::rw-,user:hadoop:rw-,group::r--,other::r--")


      try hdfs.removeACL(path: remoteFile)

      try hdfs.setACL(path: remoteFile, specification: "user::rw-,user:hadoop:rw-,group::r--,other::r--")


      try hdfs.removeACL(path: remoteFile, entries: "", defaultACL: false)

    }
    catch(WebHDFS.Exception.unexpectedResponse(let (url, header, body))) {
      XCTFail("\(op): \(url)\n\(header)\n\(body)")
    }
    catch (let err){
      XCTFail("\(op):\(err)")
    }
  }

  func testAccess() {
    let op = "testAccess"
    let hdfs = WebHDFS()
    do {
      let a = try hdfs.checkAccess(path: "/", fsaction: "mkdir")
      print("mkdir: \(a)")
      let b = try hdfs.checkAccess(path: "/", fsaction: "concat")
      print("concat: \(b)")
    }
    catch(WebHDFS.Exception.unexpectedResponse(let (url, header, body))) {
      XCTFail("\(op): \(url)\n\(header)\n\(body)")
    }
    catch (let err){
      XCTFail("\(op):\(err)")
    }
  }
  func testXAttr() {
    let op = "testXAttr"
    let hdfs = WebHDFS(user:defaultUserName)
    let now = time(nil)
    // must generate a random file to perform this test sequence
    let remoteFile = "/xattr\(now).txt"
    let localFilePath = "/tmp/xattr.txt"
    let localFile = File(localFilePath)
    do {
      try localFile.open(.write)
      try localFile.write(string: "extension attributes")
      localFile.close()
      try hdfs.create(path: remoteFile, localFile: localFilePath, overwrite: true)
      try hdfs.setXAttr(path: remoteFile, name: "user.color", value: "red")
      try hdfs.setXAttr(path: remoteFile, name: "user.size", value: "small")
      try hdfs.setXAttr(path: remoteFile, name: "user.build", value: "2016")
      try hdfs.setXAttr(path: remoteFile, name: "user.build", value: "2015", flag:.REPLACE)

      let list = try hdfs.listXAttr(path: remoteFile)
      list.forEach {
        item in
        print(item)
      }
      var a = try hdfs.getXAttr(path: remoteFile, name: ["user.color", "user.size", "user.build"])
      a.forEach{
        x in
        print("\(x.name) => \(x.value)")
      }
      XCTAssertGreaterThan(a.count, 0)
      try hdfs.removeXAttr(path: remoteFile, name: "user.size")
      a = try hdfs.getXAttr(path: remoteFile)
      a.forEach{
        x in
        print("\(x.name) => \(x.value)")
      }
      XCTAssertGreaterThan(a.count, 0)
      try hdfs.delete(path: remoteFile)
    }
    catch(WebHDFS.Exception.unexpectedResponse(let (url, header, body))) {
      XCTFail("\(op): \(url)\n\(header)\n\(body)")
    }
    catch (let err){
      XCTFail("\(op):\(err)")
    }
  }

  /// PLEASE allow snapshot before testing!
  /// $ hdfs dfsadmin -allowSnapshot /
  func testSnapshot () {
    var op = "testSnapshot"
    let hdfs = WebHDFS(user:defaultUserName)
    let dir = "/"
    let now = time(nil)
    let finalName = "snapdone\(now)"
    var snapshot: String = ""
    do {
      let (_, shortname) = try hdfs.createSnapshot(path: dir)
      snapshot = shortname
      print("_-_-_-_-_-_-_-_-_-_-_->   \(snapshot)")
      XCTAssertGreaterThan(snapshot.utf8.count, 5)
    }
    catch(WebHDFS.Exception.unexpectedResponse(let (url, header, body))) {
      XCTFail("\(op): \(url)\n\(header)\n\(body)")
    }
    catch (let err){
      XCTFail("\(op):\(err)")
    }
    op = "renameSnapshot"
    do {
      try hdfs.renameSnapshot(path: dir, from: snapshot, to: finalName)
    }
    catch(WebHDFS.Exception.unexpectedResponse(let (url, header, body))) {
      XCTFail("\(op): \(url)\n\(header)\n\(body)")
    }
    catch (let err){
      XCTFail("\(op):\(err)")
    }
    op = "deleteSnapshot"
    do {
      try hdfs.deleteSnapshot(path: dir, name: finalName)
    }
    catch(WebHDFS.Exception.unexpectedResponse(let (url, header, body))) {
      XCTFail("\(op): \(url)\n\(header)\n\(body)")
    }
    catch (let err){
      XCTFail("\(op):\(err)")
    }
  }

  func testYARNNode() {
    let ynode = YARNNodeManager()
    do {
      let i = try ynode.checkOverall()
      XCTAssertNotNil(i)
      print(i?.id ?? "")

      let apps = try ynode.checkApps()
      apps.forEach { a in
        print(" ------------  applications on node -----------")
        XCTAssertGreaterThan(a.id.utf8.count, 1)
        print(a.id)
        print(" >>>>>>>>>>>>>> check single app")
        do {
          let app = try ynode.checkApp(id: a.id)
          XCTAssertNotNil(app)
          print(app?.containerids ?? [])
          print(app?.user ?? "")
          print(app?.state ?? "")
        }catch(let error) {
            XCTFail("yarn app: \(error)")
        }//end do
      }//next

      let containers = try ynode.checkContainers()
      print(containers.count)

      containers.forEach { container in
        do {
          let con = try ynode.checkContainer(id: container.id)
          XCTAssertNotNil(con)
        }catch(let conErr) {
          XCTFail("container: \(conErr)")
        }
      }
    }catch (let err) {
      XCTFail("yarn node: \(err)")
    }
  }

  func testYARNCluster() {
    let yarn = YARNResourceManager()
    do {
      let i = try yarn.checkClusterInfo()
      XCTAssertNotNil(i)
      XCTAssertGreaterThan(i?.id ?? 0, 0)
      print("========== YARN Resource Manager -- Cluster Info ==============")
      print(i?.startedOn ?? 0)
      print(i?.state ?? "")
      print(i?.hadoopVersion ?? "")
      print(i?.resourceManagerVersion ?? "")

      let m = try yarn.checkClusterMetrics()
      XCTAssertNotNil(m)
      print(m?.availableMB ?? 0)
      print(m?.availableVirtualCores ?? 0)
      print(m?.allocatedVirtualCores ?? 0)
      print(m?.totalMB ?? 0)
      XCTAssertGreaterThan(m?.totalMB ?? 0, 0)

      let sch = try yarn.checkSchedulerInfo()
      XCTAssertNotNil(sch)
      print(sch?.capacity ?? 0.0)
      print(sch?.maxCapacity ?? 0.0)
      XCTAssertGreaterThan(sch?.capacity ?? 0.0, 0.0)
      XCTAssertGreaterThan(sch?.maxCapacity ?? 0.0, 0.0)
      print(sch?.queueName ?? "")
      print(sch?.queues.count ?? -1)

      var app = try yarn.checkApps()
      app.forEach{ a in
        print("============== YARN APP ===================")
        print(a.allocatedMB)
        print(a.allocatedVCores)
        print(a.amContainerLogs)
        print(a.amHostHttpAddress)
        print(a.amNodeLabelExpression)
        print(a.amRPCAddress)
        print(a.applicationPriority)
        print(a.applicationTags)
        XCTAssertGreaterThanOrEqual(a.amRPCAddress.utf8.count, 0)

        do {
          let state = try yarn.getApplicationStatus(id: a.id)
          XCTAssertNotEqual(state, APP.State.INVALID)
          print("= = = = = = = = = = = = = = YARN APP check state = = = = = = = = = = = = = = = = = = =")
          print(state)
          try yarn.setApplicationStatus(id: a.id, state: state)

          print("# # # # # # YARN APP check queue # # # # # #")
          let queue = try yarn.getApplicationQueue(id: a.id)
          XCTAssertGreaterThan(queue.utf8.count, 1)
          print(queue)

          try yarn.setApplicationQueue(id: a.id, queue: queue)
          
          let xapp = try yarn.checkApp(id: a.id)

          let priority = try yarn.getApplicationPriority(id: a.id)
          XCTAssertGreaterThanOrEqual(priority, 0)

          try yarn.setApplicationPriority(id: a.id, priority: priority + 1)
          
          XCTAssertNotNil(xapp)
          let x = xapp!
          print(x.allocatedMB)
          print(x.allocatedVCores)
          print(x.amContainerLogs)
          print(x.amHostHttpAddress)
          print(x.amNodeLabelExpression)
          print(x.amRPCAddress)
          print(x.applicationPriority)
          print(x.applicationTags)
          XCTAssertEqual(a.allocatedMB, x.allocatedMB)
          XCTAssertEqual(a.allocatedVCores, x.allocatedVCores)
          XCTAssertEqual(a.amContainerLogs, x.amContainerLogs)
          XCTAssertEqual(a.amHostHttpAddress, x.amHostHttpAddress)
          XCTAssertEqual(a.amNodeLabelExpression, x.amNodeLabelExpression)
          XCTAssertEqual(a.amRPCAddress, x.amRPCAddress)
          XCTAssertEqual(a.applicationPriority, x.applicationPriority)
          XCTAssertEqual(a.applicationTags, x.applicationTags)

          let attempts = try yarn.checkAppAttempts(id: a.id)
          attempts.forEach { attempt in
            print("~~~~~~~~~ YARN ATTEMPTS ~~~~~~~")
            print(attempt.containerId)
            print(attempt.id)
            print(attempt.nodeHttpAddress)
            print(attempt.nodeId)
            print(attempt.startTime)
            XCTAssertGreaterThan(attempt.id, 0)
            XCTAssertGreaterThan(attempt.containerId.utf8.count, 0)
          }
        }catch (let appErr) {
          XCTFail("YARN APP: \(appErr)")
        }


      }//next

      app = try yarn.checkApps(states: [APP.State.FINISHED, APP.State.RUNNING], finalStatus: APP.FinalStatus.SUCCEEDED)
      app.forEach{ a in
        print("============== YARN APP FILTERED ===================")
        print(a.allocatedMB)
        print(a.allocatedVCores)
        print(a.amContainerLogs)
        print(a.amHostHttpAddress)
        print(a.amNodeLabelExpression)
        print(a.amRPCAddress)
        print(a.applicationPriority)
        print(a.applicationTags)
        XCTAssertGreaterThanOrEqual(a.amRPCAddress.utf8.count, 0)
      }//next

      let sta = try yarn.checkAppStatistics(states: [APP.State.FINISHED, APP.State.RUNNING])
      sta.forEach{ s in
        print(s.count)
        print(s.state)
        print(s.type)
        XCTAssertNotEqual(s.state, APP.State.INVALID)
      }//next s

      let nodes = try yarn.checkClusterNodes()
      print("++++++++++++  YARN CLUSTER NODES ++++++++++++++")
      nodes.forEach { node in
        print(node.rack)
        print(node.availableVirtualCores)
        print(node.availMemoryMB)
        print(node.healthReport)
        print(node.healthStatus)
        print(node.id)
        print(node.lastHealthUpdate)
        print(node.nodeHostName)
        print(node.nodeHTTPAddress)
        XCTAssertGreaterThan(node.id.utf8.count, 0)
        XCTAssertGreaterThan(node.nodeHTTPAddress.utf8.count, 0)
        XCTAssertGreaterThan(node.nodeHostName.utf8.count, 0)
        print("++++++++++++  YARN CLUSTER NODES END ++++++++++++++")

        do {
          let n = try yarn.checkClusterNode(id: node.id)!
          XCTAssertEqual(node.rack, n.rack)
          XCTAssertEqual(node.availableVirtualCores, n.availableVirtualCores)
          XCTAssertEqual(node.availMemoryMB, n.availMemoryMB)
          XCTAssertEqual(node.healthReport, n.healthReport)
          XCTAssertEqual(node.healthStatus, n.healthStatus)
          XCTAssertEqual(node.lastHealthUpdate, n.lastHealthUpdate)
          XCTAssertEqual(node.nodeHostName, n.nodeHostName)
          XCTAssertEqual(node.nodeHTTPAddress, n.nodeHTTPAddress)
        }catch (let nodeErr) {
          XCTFail("Cluster Node Error: \(nodeErr)")
        }
      }
    }catch(let err) {
      XCTFail("YARN resource:\(err)")
    }
  }

  func testYarnClusterNewApp () {
    let yarn = YARNResourceManager(user: "rockywei")
    print(".................... YARN NEW APP ......................")
    do {
      let a = try yarn.newApplication()
      XCTAssertNotNil(a)
      let id = a?.id ?? ""
      let mem = a?.maximumResourceCapability.memory ?? 0
      let cores = a?.maximumResourceCapability.vCores ?? 0
      XCTAssertGreaterThan(id.utf8.count, 0)
      XCTAssertGreaterThan(mem, 0)
      XCTAssertGreaterThan(cores, 0)
      print(id)
      print(mem)
      print(cores)
      let sum = SubmitApplication()
      sum.id = id
      sum.name = "test"
      let local = LocalResource(resource: "hdfs://localhost:9000/user/rockywei/DistributedShell/demo-app/AppMaster.jar", type: .FILE, visibility: .APPLICATION, size: 43004, timestamp: 1405452071209)
      let localResources = Entries([Entry(key:"AppMaster.jar", value: local)])
      let commands = Commands("{{JAVA_HOME}}/bin/java -Xmx10m org.apache.hadoop.yarn.applications.distributedshell.ApplicationMaster --container_memory 10 --container_vcores 1 --num_containers 1 --priority 0 1><LOG_DIR>/AppMaster.stdout 2><LOG_DIR>/AppMaster.stderr")
      let environments = Entries([Entry(key:"DISTRIBUTEDSHELLSCRIPTTIMESTAMP", value: "1405459400754"), Entry(key:"CLASSPATH", value:"{{CLASSPATH}}<CPS>./*<CPS>{{HADOOP_CONF_DIR}}<CPS>{{HADOOP_COMMON_HOME}}/share/hadoop/common/*<CPS>{{HADOOP_COMMON_HOME}}/share/hadoop/common/lib/*<CPS>{{HADOOP_HDFS_HOME}}/share/hadoop/hdfs/*<CPS>{{HADOOP_HDFS_HOME}}/share/hadoop/hdfs/lib/*<CPS>{{HADOOP_YARN_HOME}}/share/hadoop/yarn/*<CPS>{{HADOOP_YARN_HOME}}/share/hadoop/yarn/lib/*<CPS>./log4j.properties"), Entry(key:"DISTRIBUTEDSHELLSCRIPTLEN", value:6), Entry(key:"DISTRIBUTEDSHELLSCRIPTLOCATION", value: "hdfs://localhost:9000/user/rockywei/demo-app/shellCommands")])
      sum.amContainerSpec = AmContainerSpec(localResources: localResources, environment: environments, commands: commands)
      sum.unmanagedAM = false
      sum.maxAppAttempts = 2
      sum.resource = ResourceRequest(memory: 1024, vCores: 1)
      sum.type = "YARN"
      sum.keepContainersAcrossApplicationAttempts = false
      sum.logAggregationContext = LogAggregationContext(logIncludePattern: "file1", logExcludePattern: "file2", rolledLogIncludePattern: "file3", rolledLogExcludePattern: "file4", logAggregationPolicyClassName: "org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation.AllContainerLogAggregationPolicy", logAggregationPolicyParameters: "")
      sum.attemptFailuresValidityInterval = 3600000
      sum.reservationId = "reservation_1454114874_1"
      sum.amBlackListingRequests = AmBlackListingRequests(amBlackListingEnabled: true, disableFailureThreshold: 0.01)
      let appUrl = try yarn.submit(application: sum)
      print(appUrl ?? "")
    }catch(let err){
      XCTFail("YARN New App:\(err)")
    }
  }

  func testBase64() {
    let hello = Base64.encode(from: "Hello, world!")
    print(hello)
    XCTAssertEqual(hello, "SGVsbG8sIHdvcmxkIQ==")
  }

  func testMapReduceHistory() {
    print(", ,, , , , , , , , HISTORY ,, ,, , , ,, , ")
    let his = MapReduceHistroy()
    do {
      let info = try his.info()
      XCTAssertNotNil(info)
      let inf = info!
      print(inf.startedOn)
      print(inf.hadoopVersion)
      print(inf.hadoopBuildVersion)
      print(inf.hadoopVersionBuiltOn)
      XCTAssertGreaterThan(inf.startedOn, 0)
    }catch(let err) {
      XCTFail("map reduce history: \(err)")
    }
  }

  func testMapReduceHistoryJobs () {
    print("^ ^ ^ ^ ^ ^ HISTORICAL JOBS  ^ ^ ^ ^ ^ ^")
    let his = MapReduceHistroy()
    do {
      let jobs = try his.checkJobs(state: .SUCCEEDED, queue: "default", limit: 10)
      XCTAssertGreaterThan(jobs.count, 0)
      jobs.forEach { j in
        print(j.id)
        print(j.name)
        print(j.queue)
        print(j.state)
        do {
          let attempts = try his.checkJobAttempts(jobId: j.id)
          XCTAssertGreaterThan(attempts.count, 0)
          attempts.forEach { attempt in
            print(attempt.id)
            print(attempt.containerId)
            print(attempt.nodeHttpAddress)
            print(attempt.nodeId)
            print(attempt.startTime)
          }
        }catch(let jobErr) {
          XCTFail("job attempt failed: \(jobErr)")
        }
      }
    }catch(let err) {
      XCTFail("map reduce historical jobs: \(err)")
    }
  }
/*
    func testToken() {
      let hdfs = WebHDFS(auth:.byDelegation(token: "TTK1234567890"))
      do {
        let token = try hdfs.getDelegationToken(renewer: defaultUserName)
        print(token)
      }
      catch(WebHDFS.WebHdfsError.unexpectedResponse(let (code, header, body))) {
        XCTFail("get token: \(code)")
        print("\(header)\n\(body)")
      }
      catch(let err) {
        XCTFail("\(err)")
      }
      do {
        let exp = try hdfs.renewDelegationToken()
        print(exp)
      }
      catch(WebHDFS.WebHdfsError.unexpectedResponse(let (code, header, body))) {
        XCTFail("renew token: \(code)")
        print("\(header)\n\(body)")
      }
      catch(let err) {
        XCTFail("\(err)")
      }
      do {
        let res = try hdfs.cancelDelegationToken()
        XCTAssertEqual(res, true)
      }
      catch(WebHDFS.WebHdfsError.unexpectedResponse(let (code, header, body))) {
        XCTFail("cancle token: \(code)")
        print("\(header)\n\(body)")
      }
      catch(let err) {
        XCTFail("\(err)")
      }

    }
    func testAuthKerb() {
      var op = "testAuthKerb"
      let hdfs = WebHDFS(user:defaultUserName)
      let dir = "/demo"
      do {
        let res = try hdfs.mkdir(path: dir, permission: 640)
        XCTAssertEqual(res, true)
        let fs = try hdfs.getFileStatus(path: dir)
        XCTAssertEqual(fs.permission, 640)
      }
      catch(WebHDFS.Exception.unexpectedResponse(let (url, header, body))) {
        XCTFail("\(op): \(url)\n\(header)\n\(body)")
      }
      catch (let err){
        XCTFail("\(op):\(err)")
      }
      op = "Kerb delete"
      do {
        let res = try hdfs.delete(path: dir)
        XCTAssertEqual(res, true)
      }
      catch(WebHDFS.Exception.unexpectedResponse(let (url, header, body))) {
        XCTFail("\(op): \(url)\n\(header)\n\(body)")
      }
      catch (let err){
        XCTFail("\(op):\(err)")
      }

    }
   */



    static var allTests : [(String, (PerfectHadoopTests) -> () throws -> Void)] {
        return [
          ("testGetFileStatus", testGetFileStatus),
               ("testDirOp", testDirOp),
               ("testFileCreateOpenDelete", testFileCreateOpenDelete),
               ("testFileAppend", testFileAppend),
               ("testFileConcat", testFileConcat),
               ("testTruncate", testTruncate),
               ("testDirectoryContentSummary", testDirectoryContentSummary),
               ("testFileCheckSum", testFileCheckSum),
               ("testHomeDirectory", testHomeDirectory),
               ("testSettings", testSettings),
               ("testACL", testACL),
               ("testAccess", testAccess),
               ("testXAttr", testXAttr),
               ("testSnapshot", testSnapshot),
               //("testToken", testToken),
          //("testAuthKerb", testAuthKerb),
          ("testYARNNode", testYARNNode),
          ("testYarnClusterNewApp", testYarnClusterNewApp),
          ("testBase64", testBase64),
          ("testMapReduceHistory", testMapReduceHistory),
          ("testMapReduceHistoryJobs", testMapReduceHistoryJobs)
        ]
    }
}
