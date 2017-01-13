# PerfectHadoop: MapReduce History Server

This project provides a Swift wrapper of MapReduce History Server REST API:

- `MapReduceHistory()`: access to all application on map / reduce history server.

## Quick Start

### Connect to Hadoop Map Reduce History Server

To connect to your Hadoop Map / Reduce History server by Perfect, initialize a `MapReduceHistory()` object with sufficient parameters:

``` swift
// this connection could possibly do some basic operations
let history = MapReduceHistory(host: "mapReduceHistory.somedomain.com", port: 19888)
```
or connect to Hadoop Map / Reduce History server with a valid user name:

``` swift
// add user name if need
let history = MapReduceHistory(host: "mapReduceHistory.somedomain.com", port: 19888, user: "your user name")
```

#### Authentication
If using Kerberos to authenticate, please try codes below:

``` swift
// set auth to kerberos
let history = MapReduceHistory(host: "mapReduceHistory.somedomain.com", port: 19888, user: "username", auth: .krb5)
```

#### Parameters of MapReduceHistory Object
- `service`:String, the service protocol of web request - http / https
- `host`:String, the hostname or ip address of the Hadoop Map Reduce history server host
- `port`:Int, the port of webhdfs host, default is 19888
- `auth`: Authorization Model, .off or .krb5. Default value is .off
- `proxyUser`:String, proxy user, if applicable
- `apibase`:String, use this parameter *ONLY* the target server has a different api routine other than `/ws/v1/history`
- `timeout`:Int, timeout in seconds, zero means never timeout during transfer
er

### Get General Information
Call `checkInfo()` to get the general information of a Hadoop MapReduce History Server in form of a `MapReduceHistory.Info` structure:

``` swift
let info = try history.checkInfo()
```

#### Members of `MapReduceHistory.Info `

Item|Data Type|Description
----|---------|-----------
startedOn|Int|The time the history server was started (in ms since epoch)
hadoopVersion|String|Version of hadoop common
hadoopBuildVersion|String|Hadoop common build string with build version, user, and checksum
hadoopVersionBuiltOn|String|Timestamp when hadoop common was built

### Check Historical MapReduce Jobs - `checkJobs()`

Call `checkJobs()` to return an array of `Job` structure.
The jobs resource provides a list of the MapReduce jobs that have finished. It does not currently return a full list of parameters. The simplest form is `checkJobs()`, which will return all jobs available:


``` swift
let jobs = try history.checkJobs()
```
#### Parameters of `checkJobs()`
Item|Data Type|Description
----|---------|-----------
**user** | String | user name
**state** | APP.FinalStatus | the job state, i.e, UNDEFINED, SUCCEEDED, FAILED and KILLED
**queue** | String | queue name
**limit** | Int | total number of app objects to be returned
**startedTimeBegin** | Int | jobs with start time beginning with this time, specified in ms since epoch
**startedTimeEnd** | Int | jobs with start time ending with this time, specified in ms since epoch
**finishedTimeBegin** | Int | jobs with finish time beginning with this time, specified in ms since epoch
**finishedTimeEnd** | Int | jobs with finish time ending with this time, specified in ms since epoch

#### Data Structure of Job
Item|Data Type|Description
----|---------|-----------
id|String|The job id
name|String|The job name
queue|String|The queue the job was submitted to
user|String|The user name
state|String|the job state - valid values are: NEW, INITED, RUNNING, SUCCEEDED, FAILED, KILL_WAIT, KILLED, ERROR
diagnostics|String|A diagnostic message
submitTime|Int|The time the job submitted (in ms since epoch)
startTime|Int|The time the job started (in ms since epoch)
finishTime|Int|The time the job finished (in ms since epoch)
mapsTotal|Int|The total number of maps
mapsCompleted|Int|The number of completed maps
reducesTotal|Int|The total number of reduces
reducesCompleted|Int|The number of completed reduces
uberized|Boolean|Indicates if the job was an uber job - ran completely in the application master
avgMapTime|Int|The average time of a map task (in ms)
avgReduceTime|Int|The average time of the reduce (in ms)
avgShuffleTime|Int|The average time of the shuffle (in ms)
avgMergeTime|Int|The average time of the merge (in ms)
failedReduceAttempts|Int|The number of failed reduce attempts
killedReduceAttempts|Int|The number of killed reduce attempts
successfulReduceAttempts|Int|The number of successful reduce attempts
failedMapAttempts|Int|The number of failed map attempts
killedMapAttempts|Int|The number of killed map attempts
successfulMapAttempts|Int|The number of successful map attempts
acls|[ACL]]|A collection of acls objects
##### Elements of the acls object
Item|Data Type|Description
----|---------|-----------
value|String|The acl value
name|String|The acl name

### Check a Specific `Job`
It is also possible to check a specific historical job by a job id:

``` swift
let job = history.checkJob(jobId: "job_1484231633049_0005")
```

See [Data Structure of Job](# Data Structure of Job) as above.