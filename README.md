# PerfectHadoop [简体中文](README.zh_CN.md)

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
        <img src="https://img.shields.io/badge/Swift-4.0-orange.svg?style=flat" alt="Swift 4.0">
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



This project provides a set of Swift classes which enable access to Hadoop servers.

This package builds with Swift Package Manager and is part of the [Perfect](https://github.com/PerfectlySoft/Perfect) project. It was written to be stand-alone and so does not require PerfectLib or any other components.

Ensure you have installed and activated the latest Swift tool chain.

## Issues

We are transitioning to using JIRA for all bugs and support related issues, therefore the GitHub issues has been disabled.

If you find a mistake, bug, or any other helpful suggestion you'd like to make on the docs please head over to [http://jira.perfect.org:8080/servicedesk/customer/portal/1](http://jira.perfect.org:8080/servicedesk/customer/portal/1) and raise it.

A comprehensive list of open issues can be found at [http://jira.perfect.org:8080/projects/ISS/issues](http://jira.perfect.org:8080/projects/ISS/issues)

## Release Note
PerfectHadoop supports Hadoop 3.0.0 with a limitation on 2.7.3.

## Building
Add this project as a dependency in your Package.swift file.

``` swift
.Package(url:"https://github.com/PerfectlySoft/Perfect-Hadoop.git", majorVersion: 3)
```

Then please add the following line to the beginning part of swift sources:
``` swift
import PerfectHadoop
```

## Error Handle - `Exception`

In case of operation failure, an exception might be thrown out. In most cases of Perfect-Hadoop, the library would probably throw a `Exception` object. User can catch it and check a tuple `(url, header, body)` of the failure, as demo below:

``` swift
do {
	// some Perfect Hadoop operations, including WebHDFS / MapReduce / YARN, all of them:
	...
}
catch(Exception.unexpectedResponse(let (url, header, body))) {
	print("Exception: \(url)\n\(header)\n\(body)")
}
catch (let err){
	print("Other Error:\(err)")
}
```

## User Manual
- WebHDFS: [Perfect-HDFS](Doc/WebHDFS.md)
- MapReduce: 
	* [Perfect-MapReduce Application Master API](Doc/MapReduceMaster.md) ⚠️ Experimental  ⚠️
	* [Perfect-MapReduce History Server API](Doc/MapReduceHistory.md)
- YARN:
	* [Perfect-YARN Node Manager](Doc/YARNNodeManager.md)
	* [Perfect-YARN Resource Manager](Doc/YARNResourceManager.md)

## Further Information
For more information on the Perfect project, please visit [perfect.org](http://perfect.org).
