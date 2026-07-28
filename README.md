# Flink Streaming Exercises

This is a **hands-on exercise project** for learning Apache Flink streaming concepts. The exercises are designed to be completed during classroom sessions.

## About This Project

Each exercise (`DataStream01`, `DataStream02`, etc.) contains **deliberately incomplete implementations** marked with TODO comments. Your task is to complete these implementations during the classroom session.

## Getting Started

### Prerequisites
- Java 8 or later
- Maven 3.6+

### Setup

1. **Import into your IDE** (IntelliJ IDEA, Eclipse, VS Code, etc.)
   - Open the project directory as a Maven project
   - Let Maven download all dependencies

2. **Run an exercise**
   - Right-click on a job class (e.g., `DataStream01DeserializeJob.java`)
   - Select "Run" or "Run with provided dependencies"
   - Maven will compile and execute the program
   - This needs to be done per each executable class (the one with `public static void main()` method)

### Build from Command Line

```bash
# Compile
mvn clean compile

# Package as fat JAR
mvn clean package

# Run a specific job
mvn exec:java -Dexec.mainClass="com.michalklempa.DataStream01DeserializeJob"
```

## Exercises Overview

### DataStream01 - execute

Try to run the exercise. It does not work. Why?

Resources:

- https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/learn-flink/datastream_api/#stream-execution-environment


### DataStream01
Deserialize JSON events to POJOs.

Resources:
 - DataStream > Operators > [map](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/operators/overview/#map)
 - [MapFunction interface](https://github.com/apache/flink/blob/master/flink-core/src/main/java/org/apache/flink/api/common/functions/MapFunction.java#L56)
 - Advanced: [RichMapFUnction](https://github.com/apache/flink/blob/master/flink-core/src/main/java/org/apache/flink/api/common/functions/RichMapFunction.java)

#### Lecturer notes
If using `ObjectMapper` there is a shaded dependency available. For the purpose of the exercise it is enough to re-use it.
For production builds, the Jackson BOM shall be imported in desired version and own Jackson import shall be used.

Readers who worked with Jackson before would soon realize that it is not plausible to instantiate Jackson's `ObjectMapper`
in each `.map` call. This is the opportunity to explore `RichMapFunction` and explain the lifecycle of serialization of code vs. transient fields
and per-executor/task `open()` method.

### DataStream02

Filter events by page type. Incoming `ClickEvent`s contain a `String` field `page`, filter out all the events, where `page` is _not_ "404".
In the latter exercises we will only be interested in measuring and working with 404 page click events.

Resources:
 - DataStream > Operators > [filter](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/operators/overview/#filter)
 - Advanced: DataStream > Operators > [flatMap](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/operators/overview/#flatmap)

We suggest first to try solve the exercise using standard `filter`. 

### DataStream03

Count events per time window (processing & event time).

### DataStream04

Per-user statistics with windowing

Each exercise builds upon the previous concepts.

Import the project into your IDE and run with provided dependencies to execute.
