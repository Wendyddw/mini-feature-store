# Spark Framework for Mini Feature Store

This directory contains the Spark and Scala framework setup for the mini feature store project.

## Features

* **Platform Abstraction**: Clean separation between Spark platform creation and business logic
* **Type-Safe Utilities**: Fetchers and Writers for common data operations
* **Test Framework**: Base trait for Spark testing with proper session management
* **Code Quality**: Pre-configured Scalafmt and Scalafix for formatting and linting
* **Production Ready**: Optimized for large-scale data processing

## Project Structure

```
spark/
├── build.sbt                    # Build configuration
├── project/
│   ├── plugins.sbt             # SBT plugins (assembly, scalafmt, scalafix)
│   └── build.properties        # SBT version
├── src/
│   ├── main/
│   │   ├── scala/
│   │   │   └── platform/   # Core Spark utilities
│   │   │       ├── SparkPlatformTrait.scala   # Trait for platform abstraction
│   │   │       ├── SparkPlatform.scala        # Production Spark session manager
│   │   │       ├── PlatformProvider.scala    # Factory for platform selection
│   │   │       ├── Fetchers.scala             # Type-safe reading utilities
│   │   │       ├── Writers.scala              # Trait for writing operations
│   │   │       └── ProdWriter.scala           # Production writer implementation
│   │   └── resources/                         # Configurations (log4j2.xml)
│   └── test/
│       ├── scala/
│       │   └── com/example/test/
│       │       ├── SparkTestBase.scala        # Base trait for parallel test execution
│       │       └── TestWriter.scala          # In-memory writer for testing
│       └── resources/                         # Test resources
├── .scalafmt.conf              # Scalafmt configuration
├── .scalafix.conf              # Scalafix configuration
├── .java-version               # Java version specification (17)
├── .gitignore                  # Git ignore patterns
└── Makefile                    # Convenient commands

```

## Prerequisites

* Java 17 (specified in `.java-version`)
* SBT 1.9.6+

## Available Commands

Run `make help` to see all available commands:

```bash
make help       # Display all available commands
make compile    # Compile the project
make test       # Run tests
make lint       # Format and fix linting issues
make test-lint  # Check formatting and linting without fixing
make format     # Format code only
make format-check # Check code formatting only
make build      # Build assembly JAR
make clean      # Clean build artifacts
```

## Usage

### Creating a Spark Platform

```scala
import platform.PlatformProvider

// Create a local Spark platform (includes both Spark session and writer)
val platform = PlatformProvider.createLocal("my-app")
val spark = platform.spark
val writer = platform.writer

// Use spark for your operations
// ...

platform.stop()
```

### Reading Data

```scala
import platform.Fetchers

// Read Parquet with optional schema
val df = Fetchers.readParquet(spark, "path/to/data.parquet")

// Read CSV
val df = Fetchers.readCsv(spark, "path/to/data.csv", header = true)
```

### Writing Data

```scala
import platform.PlatformProvider

// Create a platform (includes writer)
val platform = PlatformProvider.createLocal("my-app")
val writer = platform.writer

// Write Parquet
writer.writeParquet(df, "path/to/output", mode = "overwrite")

// Write partitioned Parquet
writer.writeParquet(df, "path/to/output", partitionBy = Seq("date", "region"))

// Write to Iceberg table
writer.insertOverwriteIcebergTable(df, "db.feature_table", partitionBy = Seq("date"))

// Write JSON
writer.writeJson(df, "path/to/output.json")

// Write CSV
writer.writeCsv(df, "path/to/output.csv", header = true, delimiter = ",")

// Or use custom writer
import platform.ProdWriter
val customWriter = new ProdWriter()
val platformWithCustomWriter = PlatformProvider.createLocal("my-app", writer = customWriter)
```

### Testing

Extend `SparkTestBase` in your test classes. This provides both a Spark session and an in-memory `TestWriter`:

```scala
import com.example.test.SparkTestBase
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class MyTest extends AnyFunSuite with SparkTestBase with Matchers {
  test("my test with in-memory writer") {
    import spark.implicits._
    val df = Seq(1, 2, 3).toDF("value")

    // Write to in-memory storage (no actual I/O)
    // testWriter is automatically available from SparkTestBase
    testWriter.writeParquet(df, "test/path")

    // Retrieve the stored DataFrame
    val stored = testWriter.getStoredData("test/path")
    stored should be(defined)
    stored.get.count() should be(3)

    // Check all stored keys
    testWriter.getAllStoredKeys should contain("test/path")

    // You can also use platform.writer (same instance)
    platform.writer.writeJson(df, "test/json")
  }

  test("test Iceberg table writes") {
    import spark.implicits._
    val df = Seq(("user1", 0.5), ("user2", 0.8)).toDF("user_id", "score")

    testWriter.insertOverwriteIcebergTable(df, "test_db.features")

    val stored = testWriter.getStoredData("test_db.features")
    stored.get.count() should be(2)
  }
}
```

The `TestWriter` stores all DataFrames in memory, making tests fast and isolated without requiring actual storage systems.

## Key Principles

1. **Platform Abstraction**: Use `SparkPlatformTrait` to abstract Spark session creation
2. **Type Safety**: Use case classes and explicit schemas for data transformations
3. **Pure Functions**: All transformations should be pure with no side effects
4. **Testing**: All logic should be covered by tests

## Building for Deployment

Create a fat JAR for cluster deployment:

```bash
make build
```

The assembled JAR will be created in `target/scala-2.13/` and can be submitted to a Spark cluster using `spark-submit`.

