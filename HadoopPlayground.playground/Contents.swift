import Darwin
import PerfectLib

extension Double {
  public init(any: Any?) {
    switch any {
    case is Int:
      self = Double(any as? Int ?? 0)
    default:
      self = any as? Double ?? 0.0
    }//end case
  }//end init
}//end Double

public struct Container{
  var containerLogsLink = ""
  var diagnostics = ""
  var exitCode = 0
  var id = ""
  var nodeId = ""
  var state = ""
  var totalMemoryNeededMB = 0
  var totalVCoresNeeded = 0
  var user = ""
  public init(_ dictionary: [String:Any] = [:]) {
    self.containerLogsLink = dictionary["containerLogsLink"] as? String ?? ""
    self.diagnostics = dictionary["diagnostics"] as? String ?? ""
    self.exitCode = dictionary["exitCode"] as? Int ?? 0
    self.id = dictionary["id"] as? String ?? ""
    self.nodeId = dictionary["nodeId"] as? String ?? ""
    self.state = dictionary["state"] as? String ?? ""
    self.totalMemoryNeededMB = dictionary["totalMemoryNeededMB"] as? Int ?? 0
    self.totalVCoresNeeded = dictionary["totalVCoresNeeded"] as? Int ?? 0
    self.user = dictionary["user"] as? String ?? ""
  }//init
}//Container

extension String {
  public var asContainers: [Container] {
    get{
      do{
        let dic = try self.jsonDecode() as? [String:Any] ?? [:]
        let c = dic["containers"] as? [String:Any] ?? [:]
        return (c["container"] as? [Any] ?? []).map { Container ($0 as? [String:Any] ?? [:])}
      }catch{
        return []
      }//end do
    }//end get
  }//end member
}//end extension


do {
  let f = File("/tmp/json/containers.json")
  try f.open(.read)
  let json = try f.readString()
  f.close()
  let cn = json.asContainers
  cn.forEach { c in
    print("=================")
    print(c.nodeId)
    print(c.totalMemoryNeededMB)
    print(c.totalVCoresNeeded)
    print(c.state)
    print(c.diagnostics)
    print(c.containerLogsLink)
    print(c.user)
    print(c.id)
    print(c.exitCode)
  }
}catch (let err){
  print(err)

}


