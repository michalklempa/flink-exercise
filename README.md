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
mvn compile exec:java -Dexec.mainClass="com.michalklempa.flink.exercise.datastream.DataStream01DeserializeJob"

# Run a reference solution
mvn compile exec:java -Dexec.mainClass="com.michalklempa.flink.solution.datastream.DataStream01DeserializeSolution"
```

Flink dependencies have `provided` scope, so they are not part of the fat JAR. The `exec-maven-plugin` in `pom.xml`
is configured with `classpathScope` `test`, which puts the `provided` dependencies on the classpath when running via Maven.
Stop a running job with `Ctrl+C`.

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
 - Advanced: [RichMapFunction](https://github.com/apache/flink/blob/master/flink-core/src/main/java/org/apache/flink/api/common/functions/RichMapFunction.java)

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

Count 404 page views per one-minute tumbling window, first in processing time, then in event time.
The non-keyed `windowAll` is used, so the whole stream is counted together. Print the window start and end together with the count.

Resources:
 - DataStream > Operators > [Windows](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/operators/windows/)
 - Window functions: [ReduceFunction](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/operators/windows/#reducefunction), [AggregateFunction](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/operators/windows/#aggregatefunction), [ProcessWindowFunction](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/operators/windows/#processwindowfunction)
 - Concepts > [Timely Stream Processing](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/concepts/time/)
 - [Generating Watermarks](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/event-time/generating_watermarks/)

#### Lecturer notes
Solutions `DataStream03aCount404ProcessingTimeSolution` and `DataStream03bCount404EventTimeSolution` walk through several
approaches, from the least to the most efficient: `ProcessAllWindowFunction` buffering all elements, `reduce`,
`aggregate`, and `aggregate` combined with a process function to capture window boundaries.

The event-time variant differs only in assigning timestamps and watermarks prior to windowing and using `TumblingEventTimeWindows`.
Point out that the window timestamps in the output are from the data, not from the wall clock, and that windows fire
without waiting a real minute.

### DataStream04

Count page views per user per one-minute window (processing & event time). In comparison to DataStream03, the stream
is partitioned with `keyBy` on `user_id` first.

Resources:
 - DataStream > Operators > [keyBy](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/operators/overview/#keyby)
 - DataStream > Operators > [Keyed Windows](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/operators/windows/#keyed-vs-non-keyed-windows)
 - [ProcessWindowFunction](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/operators/windows/#processwindowfunction) with the key as the first argument

#### Lecturer notes
Key classes must have proper `hashCode` and `equals`. This is the opportunity to explain how keys decide which parallel
task owns the data, and what happens to the state of a window per key.

### DataStream05 - capstone

Figure out the share of 404 page views over all page views for each user, per one-hour tumbling window in event time.
It combines deserialization, keyed streams, event-time windows and watermarks.

Unlike in the previous exercises, the stream must not be filtered, because the denominator is all page views.

Resources:
 - [AggregateFunction](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/operators/windows/#aggregatefunction)
 - [ProcessWindowFunction with incremental aggregation](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/operators/windows/#processwindowfunction-with-incremental-aggregation)

#### Lecturer notes
The generated data advances by 421 ms of event time per event, so one hour is around 8500 events. With the generator rate of
100 events per second, expect to wait about 90 seconds for the first window to fire.

`DataStream05CountShareOf404PerUserSolution` first shows the simple solution, which buffers the whole window and counts
after the trigger fires, then the solution with an `AggregateFunction` whose accumulator is a pair of counters (404s, all).
The share is computed only at the end, because shares cannot be merged but counters can.

### WordCount

Classic word count on an unbounded stream of sentences, outside of the numbered track. Split sentences into words,
count occurrences per word and print the running counts.

Classes: `com.michalklempa.flink.exercise.wordcount.WordCountJob`, solution `com.michalklempa.flink.solution.wordcount.WordCountSolution`.

Resources:
 - DataStream > Operators > [flatMap](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/operators/overview/#flatmap)
 - DataStream > Operators > [keyBy](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/operators/overview/#keyby)
 - DataStream > Operators > [Reduce / sum](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/operators/overview/#reduce)

#### Lecturer notes
There are no windows here, the counts are running totals kept in keyed state by the `sum` operator.

Each exercise builds upon the previous concepts.

Import the project into your IDE and run with provided dependencies to execute.
