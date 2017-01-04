import Darwin
import PerfectLib

public struct Filestatus{
  public struct ownersItem{
    var id = ""
    var name = ""
    public init(_ dictionary: [String:Any] = [:]) {
      self.id = dictionary["id"] as? String ?? ""
      self.name = dictionary["name"] as? String ?? ""
    }//init
  }//ownersItem
  var accessTime = 0
  var blockSize = 0
  var group = ""
  var length = 0
  var modificationTime = 0
  var owner = ""
  var owners : [ownersItem] = []
  var pathSuffix = ""
  var permission = ""
  var replication = 0
  var tags : [String] = []
  var type = ""
  public init(_ dictionary: [String:Any] = [:]) {
    self.accessTime = dictionary["accessTime"] as? Int ?? 0
    self.blockSize = dictionary["blockSize"] as? Int ?? 0
    self.group = dictionary["group"] as? String ?? ""
    self.length = dictionary["length"] as? Int ?? 0
    self.modificationTime = dictionary["modificationTime"] as? Int ?? 0
    self.owner = dictionary["owner"] as? String ?? ""
    self.owners = (dictionary["owners"] as? [Any] ?? []).map{ownersItem($0 as? [String : Any] ?? [:])}
    self.pathSuffix = dictionary["pathSuffix"] as? String ?? ""
    self.permission = dictionary["permission"] as? String ?? ""
    self.replication = dictionary["replication"] as? Int ?? 0
    self.tags = dictionary["tags"] as? [String] ?? []
    self.type = dictionary["type"] as? String ?? ""
  }//init
}//Filestatus

extension String {
  public var asFilestatus: Filestatus? {
    get{
      do{
        let dic = try self.jsonDecode() as? [String:Any] ?? [:]
        return Filestatus(dic["FileStatus"] as? [String:Any] ?? [:])
      }catch{
        return nil
      }//end do
    }//end get
  }//end member
}//end extension
/*
do {
  let f = File("/tmp/json/ClusterInfo.json")
  try f.open(.read)
  let json = try f.readString()
  f.close()
  let j = json.asApp!
  print(j.clusterId)
  print(j.id)
  print(j.progress)
  print(j.startedTime)
  print(j.elapsedTime)
  print(j.amContainerLogs)
  print(j.trackingUrl)
}catch {


}
*/