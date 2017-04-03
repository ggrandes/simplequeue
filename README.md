# SimpleQueue

SimpleQueue is a HTTP PUT/GET Queue Manager in Java. Project is Open source (Apache License, Version 2.0).

### Current Stable Version is [1.0.0](https://maven-release.s3.amazonaws.com/release/org/javastack/simplequeue/1.0.0/simplequeue-1.0.0.war)

---

## DOC

### Supported features

  - [x] Put data
  - [x] Get data with timeout
  - [x] Get queue depth/size

### Limits

  - Queue names are limited to safe URL caracters: `A-Za-z0-9._-`
  - Queues are stored in a single directory, in some filesystems, like FAT32 are limited to 65k files.
  - If you use a case-insensitive filesystem (like FAT32) Queue names can collide.

### Configuration

SimpleQueue only need directory path to store data, this is configured with property name `org.javastack.simplequeue.directory`, that can be configured in a Context Param, System Property, System Environment, or file named `simplequeue.properties` (located in classpath) 

### Sample cURL usage

```bash
# PUT "test data" in queue "q1"
curl -X PUT -H "Content-Type: text/plain" --data-binary "test data" http://localhost:8080/simplequeue/q1

# GET data from queue "q1" with a timeout of 3000 milliseconds
curl -X GET http://localhost:8080/simplequeue/q1?timeout=3000

# GET depth of queue "q1"
curl -X GET http://localhost:8080/simplequeue/q1?size
```

---
Inspired in [ActiveMQ](http://activemq.apache.org/), this code is Java-minimalistic version.
