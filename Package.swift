import PackageDescription
#if os(OSX)
import Darwin
#else
import Glibc
#endif

let urlHttp: String 
let urlCurl: String
if let cache = getenv("URL_PERFECT") {
  let local = String(cString: cache)
  urlHttp = "\(local)/Perfect-HTTP"
  urlCurl = "\(local)/Perfect-CURL"
} else {
  urlHttp = "https://github.com/PerfectlySoft/Perfect-HTTP.git"
  urlCurl = "https://github.com/PerfectlySoft/Perfect-CURL.git"
}
let package = Package(
    name: "PerfectHadoop",
    targets: [],
    dependencies: [
        .Package(url: urlHttp, majorVersion: 3),
        .Package(url: urlCurl, majorVersion: 3),
    ]
)