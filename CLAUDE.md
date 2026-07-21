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
mvn exec:java -Dexec.mainClass="com.michalklempa.DataStreamXXJob"
```

Or compile to JAR and run with Flink:
```bash
./flink run target/flink-exercise-1.0.0-SNAPSHOT.jar
```

**Main class configuration**: Set in `pom.xml` `<transformers><mainClass>` under maven-shade-plugin. Update when changing the entry point.

## Architecture

### Job Structure
- Progressive exercises: `DataStream01DeserializeJob` → `DataStream02FilterJob` → `DataStream03Count404Job` → ...
- Each job has TODO comments marking incomplete sections
- Standard pattern: `StreamExecutionEnvironment.getExecutionEnvironment()` → source → transformations → execute

### Data Model
- **ClickEvent** (`src/main/java/com/michalklempa/ClickEvent.java`): POJO with `page` (String) and `user_id` (Long)
  - Renders as JSON via Apache Commons ToStringBuilder
  - Pages: home, cart, checkout, detail, registration, login, 404

- **JsonSourceFactory**: Creates bounded data generator source
  - Generates 60 events/second (rate limited)
  - Returns JSON strings: `{"page":"home","user_id":42}`
  - Used by all jobs via `env.fromSource(JsonSourceFactory.create(), ...)`

### Time Semantics
- **Processing Time** (default): No watermarks. Use `WatermarkStrategy.noWatermarks()`
- **Event Time**: Assign watermarks via `WatermarkStrategy` with timestamp extraction. Enables window operations based on event timestamps rather than arrival time.
- Distinction: Check watermark strategy in job setup. `noWatermarks()` = processing time; explicit `WatermarkStrategy` = event time

### Configuration
- **Parallelism**: Set to 1 in all jobs (`env.setParallelism(1)`)
- **Watermarks**: Configured per-source via second argument to `env.fromSource()`
- **Dependencies**: Flink streaming, clients, connector-base, connector-datagen (only datagen is runtime; others are provided scope)
- **Logging**: Log4j 2.24.3 via log4j-slf4j-impl (configured in `src/main/resources/log4j2.properties`)

## Key Implementation Details

- Jobs use fluent API: chain operations (filter, map, print, etc.) on DataStream
- No test files yet—exercises are self-contained jobs
- JSON deserialization typically uses Flink's built-in ObjectMapper or custom parsing
- Window operations (tumbling, sliding) require time assignment and watermarks
