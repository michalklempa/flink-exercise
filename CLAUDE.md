# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Flink Streaming Exercises**: Progressive hands-on exercises for Apache Flink 1.18.1 using Java 8. Each DataStream job builds upon previous concepts, from basic deserialization through windowing and time semantics.

## Build & Commands

| Command | Purpose |
|---------|---------|
| `mvn clean compile` | Compile without building JAR |
| `mvn clean package` | Build fat JAR (with maven-shade plugin) |
| `mvn verify` | Compile + test |

**Output**: Fat JAR in `target/flink-exercise-1.0.0-SNAPSHOT.jar`

## Running Jobs

Each job is a standalone Java program with a `main()` method. To run locally:

```bash
mvn exec:java -Dexec.mainClass="com.michalklempa.flink.exercise.datastream.DataStream01DeserializeJob"
```

Or compile to JAR and run with Flink:
```bash
./flink run target/flink-exercise-1.0.0-SNAPSHOT.jar
```

**Main class configuration**: Set in `pom.xml` under maven-shade-plugin > transformers > ManifestResourceTransformer > mainClass. Update when changing the entry point (default: `DataStream01DeserializeJob`).

## Architecture

### Job Structure
- **Exercise jobs** (incomplete, TODO-driven): `com.michalklempa.flink.exercise.datastream.DataStream01DeserializeJob` → `DataStream02FilterJob` → `DataStream03Count404Job` → `DataStream04PerUserStatsJob` → `DataStream05CountShareOf404PerUserJob`
- **Solution jobs** (reference implementations): Parallel solutions in `com.michalklempa.flink.solution.datastream.*Solution` packages
- **Wordcount exercise**: `com.michalklempa.flink.exercise.wordcount.WordCountJob` (separate track)
- Each exercise job has TODO comments marking incomplete sections
- Standard pattern: `StreamExecutionEnvironment.getExecutionEnvironment()` → source → transformations → execute

### Data Model
- **PageViewEvent** (`src/main/java/com/michalklempa/flink/exercise/datastream/PageViewEvent.java`): POJO with fields:
  - `client_time` (long): Event timestamp in milliseconds (e.g., 1784541600000L)
  - `page` (String): Page type (home, cart, checkout, detail, registration, login, 404)
  - `user_id` (long): User identifier (0-99)
  - Renders as JSON via Apache Commons ToStringBuilder: `{"client_time":1784541600000,"page":"home","user_id":42}`

- **JsonSourceFactory** (exercise & solution variants): Creates bounded data generator source
  - Generates 60 events/second (rate limited via RateLimiterStrategy)
  - Returns JSON strings (PageViewEvent serialized)
  - Used by all jobs via `env.fromSource(JsonSourceFactory.create(), WatermarkStrategy.*, "Json Source")`

### Time Semantics
- **Processing Time** (default): No watermarks. Use `WatermarkStrategy.noWatermarks()`
- **Event Time**: Assign watermarks via `WatermarkStrategy` with timestamp extraction. Enables window operations based on event timestamps rather than arrival time.
- Distinction: Check watermark strategy in job setup. `noWatermarks()` = processing time; explicit `WatermarkStrategy` = event time

### Configuration
- **Parallelism**: Set to 1 in all jobs (`env.setParallelism(1)`)
- **Watermarks**: Configured per-source via second argument to `env.fromSource()`. Use `WatermarkStrategy.noWatermarks()` for processing time, explicit strategy for event time.
- **Dependencies**: 
  - Flink: streaming-java, clients, connector-base (provided scope, embedded in distribution)
  - flink-connector-datagen (runtime scope for built JAR)
  - Log4j 2.24.3 via log4j-slf4j-impl (runtime scope)
  - Jackson (Flink-shaded): Use `org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper` for JSON deserialization
- **Logging**: Configured in `src/main/resources/log4j2.properties`

## Key Implementation Details

- **API Style**: Fluent DataStream API—chain operations (map, filter, print, aggregate, windowAll, etc.)
- **JSON Deserialization**: Use Flink-shaded Jackson `ObjectMapper.readValue(jsonString, PageViewEvent.class)`. Can be inlined in .map() or wrapped in RichMapFunction for serialization control.
- **Window Operations**: Require proper time semantics (processing vs event time) and watermarks. Event time windows must extract timestamp via WatermarkStrategy.
- **Execution**: All jobs call `env.execute(jobName)` to submit to local/distributed Flink runtime
- **No Tests**: Exercises are self-contained jobs; verification is via log output and print() operators
- **Solution Reference**: Solution packages provide complete implementations for comparison
