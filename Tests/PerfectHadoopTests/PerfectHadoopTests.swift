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
    }catch(let err) {
      XCTFail("YARN resource:\(err)")
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
          ("testYARNNode", testYARNNode)
        ]
    }
}
