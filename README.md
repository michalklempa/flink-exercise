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

- **DataStream01**: Deserialize JSON events to POJOs
- **DataStream02**: Filter events by page type
- **DataStream03**: Count events per time window (processing & event time)
- **DataStream04**: Per-user statistics with windowing

Each exercise builds upon the previous concepts.

## Solution Reference

Solution implementations (with `Solution` suffix, e.g., `DataStream01DeserializeSolution`) are provided as reference after completing the exercises.

---

**Import the project into your IDE and run with provided dependencies to execute.**
