# SDKs Microbenchmarks
Programs contained in this directory are specific purpose microbenchs aims at measuring specific SDK features.
In general, any program should be able to process commandline parameters listed in `perfrunner/workloads/sdk_benchs.py`.
A good practice is to also print a warning for any unrecognised options, incase new options are added to the benchmark runner.

## Directory Structure
Here is an example directory structure for each supported SDK
```
sdks
├── README.md
├── dotnet                       # <-- SDK type (sdk_type)
│   └── config_push              # <-- Benchmark name (bench_name)
│       ├── Program.cs
│       ├── config_push.csproj
├── go
│   ├── config_push.go
├── java
│   └── config-push
│       ├── pom.xml
│       ├── src
│       │   └── main
│       │       └── java
│       │           └── com
│       │               └── couchbase
│       │                   └── qe
│       │                       └── perf
│       │                           └── Main.java
├── libc
│   ├── config_push.c           # <-- Benchmark name (bench_name)
│   └── core                    # <-- Shared code
│       └── benchmark.h
└── python
    └── config_push.py
```
Other than Java and .Net where benchmark name translate to a subdirectory inside the SDK directory,
most other languages it is the code file name.

## Test Configuration
The following example shows an example test configuration which translates to a test present
in the above directory structure.
```
[sdktesting]
enable_sdktest = 1
sdk_timeout = 10
sdk_type = java
bench_name = config-push

[clients]
libcouchbase = 3.3.8  # We use pillowfight for loading data in these tests
java_client = 3.4.10
```

## Compiling a Benchmark
### .Net
Program is compiled as:
```
dotnet add {bench_name} package CouchbaseNetClient --version {version}
dotnet build {bench_name}
```

### Go
Program is compiled as:
```
go mod init github.com/couchbase/perfrunner/{bench_name}
go get github.com/couchbase/gocb/v2@{version}
go mod tidy
go build .
```

### Java
Java program need to contain `com.couchbase.qe.perf.Main` class as a starting point.
Program is compiled as:
```
mvn package dependency:build-classpath  \
-Dmdep.outputFilterFile=true -Dmdep.outputFile=cp.txt \
"-Dcouchbase3.version={version}"
```

### LCB
The version of LCB is installed the same way as any other perfrunner test. The program is then compiled as:
```
cc -o {bench_name} {bench_name}.c -lcouchbase -lpthread -lm
```

## Running a Benchmark
Refer to `perfrunner/workloads/sdk_benchs.py` to see how each language program is run.
But each program is given the following parameters:
```
--host {host}
--bucket {bucket}
--username {username}
--password {password}
--num-items {num_items}  # In most languages this can be derived from keys
--num-threads {num_threads}
--timeout {timeout}  # SDK KV timeout
--keys {keys}  # A string with doc keys separated by comma ','
--time {time}  # How long to run the program
--c-poll-interval {config_poll_interval}
```