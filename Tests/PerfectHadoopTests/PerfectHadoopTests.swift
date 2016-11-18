import XCTest
import PerfectLib

@testable import PerfectHadoop

class PerfectHadoopTests: XCTestCase {

    let defaultUserName:String = "hdp"

    func testGetFileStatus() {
      let op = "testGetFileStatus"

      let hdfs = WebHDFS()
      do {
        let fs = try hdfs.getFileStatus(path: "/")
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
      let hdfs = WebHDFS(auth:.byUser(name: defaultUserName))
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
      op = "rmdir"
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

  		func testFileCreateOpenDelete() {
        let op = "testFileCreateOpenDelete"
        let hdfs = WebHDFS(auth:.byUser(name: defaultUserName))
        let remoteFile = "/a.txt"
        let localFilePath = "/tmp/a.txt"
        let localFile = File(localFilePath)
        do {
          try localFile.open(.write)
          try localFile.write(string: "hello")
          localFile.close()
          var res = try hdfs.create(path: remoteFile, localFile: localFilePath, overwrite: true)
          XCTAssertEqual(res, true)
          let bytes = try hdfs.openFile(path: remoteFile)
          XCTAssertEqual(bytes.count, 5)
          res = try hdfs.delete(path: remoteFile)
          XCTAssertEqual(res, true)
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
      let hdfs = WebHDFS(auth:.byUser(name: defaultUserName))
      let remoteFile = "/b.txt"
      let localFilePath = "/tmp/b.txt"
      let localFile = File(localFilePath)
      do {
        try localFile.open(.write)
        try localFile.write(string: "had0p")
        localFile.close()
        var res = try hdfs.create(path: remoteFile, localFile: localFilePath, overwrite: true)
        XCTAssertEqual(res, true)
        res = try hdfs.append(path: remoteFile, localFile: localFilePath)
        XCTAssertEqual(res, true)
        let bytes = try hdfs.openFile(path: remoteFile)
        XCTAssertEqual(bytes.count, 10)
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
      let hdfs = WebHDFS(auth:.byUser(name: defaultUserName))
      let localFilePath = "/tmp/c.txt"
      let files = ["/1.txt", "/2.txt"]
      let remoteFile = "/0.txt"
      let localFile = File(localFilePath)
      do {
        try localFile.open(.write)
        try localFile.write(string: "12345")
        localFile.close()
        var res = try hdfs.create(path: files[0], localFile: localFilePath, overwrite: true)
        XCTAssertEqual(res, true)
        res = try hdfs.create(path: files[1], localFile: localFilePath, overwrite: true)
        XCTAssertEqual(res, true)
        res = try hdfs.create(path: remoteFile, localFile: localFilePath, overwrite: true)
        XCTAssertEqual(res, true)
        res = try hdfs.concat(path: remoteFile, sources: files)
        XCTAssertEqual(res, true)
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
      let hdfs = WebHDFS(auth:.byUser(name: defaultUserName))
      let remoteFile = "/d.txt"
      let localFilePath = "/tmp/d.txt"
      let localFile = File(localFilePath)
      do {
        try localFile.open(.write)
        try localFile.write(string: "1234567890123456789012345678901234567890123")
        localFile.close()
        var res = try hdfs.create(path: remoteFile, localFile: localFilePath, overwrite: true)
        XCTAssertEqual(res, true)
        res = try hdfs.truncate(path: remoteFile, newlength:5)
        XCTAssertEqual(res, true)
        let bytes = try hdfs.openFile(path: remoteFile)
        XCTAssertGreaterThan(bytes.count, 5)
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
      let hdfs = WebHDFS(auth:.byUser(name: defaultUserName))
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
      let hdfs = WebHDFS(auth:.byUser(name: defaultUserName))
      do {
        let list = try hdfs.getDirectoryContentSummary(path: "/")
        XCTAssertGreaterThan(list.fileCount, 0)
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
      let hdfs = WebHDFS(auth:.byUser(name: defaultUserName))
      let remoteFile = "/checksum.txt"
      let localFilePath = "/tmp/checksum.txt"
      let localFile = File(localFilePath)
      do {
        try localFile.open(.write)
        try localFile.write(string: "000000000000000")
        localFile.close()
        let res = try hdfs.create(path: remoteFile, localFile: localFilePath, overwrite: true)
        XCTAssertEqual(res, true)
        let checksum = try hdfs.getFileCheckSum(path: remoteFile)
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
     let hdfs = WebHDFS(auth:.byUser(name: defaultUserName))
      let remoteFile = "/settings.txt"
      let localFilePath = "/tmp/settings.txt"
      let localFile = File(localFilePath)
      do {
        try localFile.open(.write)
        try localFile.write(string: "configuration")
        localFile.close()
        var res = try hdfs.create(path: remoteFile, localFile: localFilePath, overwrite: true)
        XCTAssertEqual(res, true)
        let perm = try hdfs.setPermission(path: remoteFile, permission: 640)
        XCTAssertEqual(perm, true)
        res = try hdfs.setOwner(path: remoteFile, name: defaultUserName, group: defaultUserName)
        XCTAssertEqual(res, true)
        res = try hdfs.setReplication(path: remoteFile, factor: 2)
        XCTAssertEqual(res, true)
        res = try hdfs.setTimes(path: remoteFile, modification: 1478000000)
        XCTAssertEqual(res, true)
        res = try hdfs.setTimes(path: remoteFile, access: 1478000000)
        XCTAssertEqual(res, true)
      }
      catch(WebHDFS.Exception.unexpectedResponse(let (url, header, body))) {
        XCTFail("\(op): \(url)\n\(header)\n\(body)")
      }
      catch (let err){
        XCTFail("\(op):\(err)")
      }
    }
    func testACL() {
      let op = "testACL"
      let hdfs = WebHDFS(auth:.byUser(name: defaultUserName))
      let remoteFile = "/acl.txt"
      let localFilePath = "/tmp/acl.txt"
      let localFile = File(localFilePath)
      do {
        try localFile.open(.write)
        try localFile.write(string: "acl test file")
        localFile.close()
        var res = try hdfs.create(path: remoteFile, localFile: localFilePath, overwrite: true)
        XCTAssertEqual(res, true)

        var acl = try hdfs.getACL(path: remoteFile)
        print(acl.group)
        print(acl.owner)
        print(acl.entries)
        print(acl.permission)
        print(acl.stickyBit)
        XCTAssertGreaterThan(acl.permission, 0)

        res = try hdfs.setACL(path: remoteFile, specification: "user::rw-,user:hadoop:rw-,group::r--,other::r--")
        XCTAssertEqual(res, true)

        acl = try hdfs.getACL(path: remoteFile)
        print(acl.group)
        print(acl.owner)
        print(acl.entries)
        print(acl.permission)
        print(acl.stickyBit)
        XCTAssertGreaterThan(acl.permission, 0)

        res = try hdfs.modifyACL(path: remoteFile, entries: "user::rwx,user:hadoop:rwx,group::rwx,other::---")
        XCTAssertEqual(res, true)

        acl = try hdfs.getACL(path: remoteFile)
        print(acl.group)
        print(acl.owner)
        print(acl.entries)
        print(acl.permission)
        print(acl.stickyBit)
        XCTAssertGreaterThan(acl.permission, 0)

        res = try hdfs.removeACL(path: remoteFile, defaultACL: false)
        XCTAssertEqual(res, true)


        res = try hdfs.setACL(path: remoteFile, specification: "user::rw-,user:hadoop:rw-,group::r--,other::r--")
        XCTAssertEqual(res, true)


        res = try hdfs.removeACL(path: remoteFile)
        XCTAssertEqual(res, true)

        res = try hdfs.setACL(path: remoteFile, specification: "user::rw-,user:hadoop:rw-,group::r--,other::r--")
        XCTAssertEqual(res, true)


        res = try hdfs.removeACL(path: remoteFile, entries: "", defaultACL: false)
        XCTAssertEqual(res, true)

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
        var res = try hdfs.checkAccess(path: "/", fsaction: "mkdir")
        XCTAssertTrue(res)
        res = try hdfs.checkAccess(path: "/", fsaction: "concat")
        XCTAssertTrue(res)
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
     let hdfs = WebHDFS(auth:.byUser(name: defaultUserName))
      let now = time(nil)
      // must generate a random file to perform this test sequence
      let remoteFile = "/xattr\(now).txt"
      let localFilePath = "/tmp/xattr.txt"
      let localFile = File(localFilePath)
      do {
        try localFile.open(.write)
        try localFile.write(string: "extension attributes")
        localFile.close()
        var res = try hdfs.create(path: remoteFile, localFile: localFilePath, overwrite: true)
        XCTAssertEqual(res, true)
        res = try hdfs.setXAttr(path: remoteFile, name: "user.color", value: "red")
        XCTAssertEqual(res, true)
        res = try hdfs.setXAttr(path: remoteFile, name: "user.size", value: "small")
        XCTAssertEqual(res, true)
        res = try hdfs.setXAttr(path: remoteFile, name: "user.build", value: "2016")
        XCTAssertEqual(res, true)
        res = try hdfs.setXAttr(path: remoteFile, name: "user.build", value: "2015", flag:.REPLACE)
        XCTAssertEqual(res, true)

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
        res = try hdfs.removeXAttr(path: remoteFile, name: "user.size")
        XCTAssertEqual(res, true)
        a = try hdfs.getXAttr(path: remoteFile)
        a.forEach{
          x in
          print("\(x.name) => \(x.value)")
        }
        XCTAssertGreaterThan(a.count, 0)
        res = try hdfs.delete(path: remoteFile)
        XCTAssertEqual(res, true)
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
      let hdfs = WebHDFS(auth:.byUser(name: defaultUserName))
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
        let res = try hdfs.renameSnapshot(path: dir, from: snapshot, to: finalName)
        XCTAssertEqual(res, true)
      }
      catch(WebHDFS.Exception.unexpectedResponse(let (url, header, body))) {
        XCTFail("\(op): \(url)\n\(header)\n\(body)")
      }
      catch (let err){
        XCTFail("\(op):\(err)")
      }
      op = "deleteSnapshot"
      do {
        let res = try hdfs.deleteSnapshot(path: dir, name: finalName)
        XCTAssertEqual(res, true)
      }
      catch(WebHDFS.Exception.unexpectedResponse(let (url, header, body))) {
        XCTFail("\(op): \(url)\n\(header)\n\(body)")
      }
      catch (let err){
        XCTFail("\(op):\(err)")
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
    */
    func testAuthKerb() {
      var op = "testAuthKerb"
      let hdfs = WebHDFS(auth:.byKerberos(name: defaultUserName))
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
    }//end testAuthKerb

  func testYarn() {
    let op = "yarn"
    let yarn = WebYarn()
    do {
      let app = try yarn.appStatus(appId: "application_1479494861448_0003")
      for (k,v) in app {
        print(" \(k) -> \(v)")
      }
      XCTAssertNotNil(app["id"])
    }
    catch(WebHDFS.Exception.unexpectedResponse(let (url, header, body))) {
      XCTFail("\(op): \(url)\n\(header)\n\(body)")
    }
    catch (let err){
      XCTFail("\(op):\(err)")
    }
  }

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
        ("testAuthKerb", testAuthKerb),
      ]
    }
}
