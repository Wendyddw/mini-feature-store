# Spark Framework for Mini Feature Store

Spark and Scala framework for the mini feature store project.

## Features

* **Platform Abstraction**: Clean separation between Spark platform creation and business logic
* **Type-Safe Utilities**: Fetchers and Writers for common data operations, encapsulated in platform
* **Test Framework**: Base trait for Spark testing with in-memory TestFetcher and TestWriter
* **E2E Testing**: End-to-end tests following Arrange-Action-Assert pattern
* **Code Quality**: Pre-configured Scalafmt and Scalafix for formatting and linting

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
│   │   │       ├── Fetchers.scala             # Trait for reading operations
│   │   │       ├── ProdFetcher.scala          # Production fetcher implementation
│   │   │       ├── Writers.scala              # Trait for writing operations
│   │   │       └── ProdWriter.scala           # Production writer implementation
│   │   └── resources/                         # Configurations (log4j2.xml)
│   └── test/
│       ├── scala/
│       │   └── com/example/featurestore/suit/
│       │       ├── SparkTestBase.scala        # Base trait for parallel test execution
│       │       ├── TestWriter.scala           # In-memory writer for testing
│       │       └── TestFetcher.scala          # In-memory fetcher for testing
│       └── resources/                         # Test resources
├── .scalafmt.conf              # Scalafmt configuration
├── .scalafix.conf              # Scalafix configuration
├── .java-version               # Java version specification (17)
├── .gitignore                  # Git ignore patterns
└── Makefile                    # Convenient commands

```

## Prerequisites

* Java 17+ (required for Spark 3.5.0 compatibility)
* SBT 1.9.6+

## Available Commands

```bash
make help         # Display all available commands
make compile      # Compile the project
make test         # Run tests
make lint         # Format and fix linting issues
make test-lint    # Check formatting and linting without fixing
make format       # Format code only
make format-check # Check code formatting only
make build        # Build assembly JAR
make clean        # Clean build artifacts
```

## Usage

### Creating a Spark Platform

```scala
import platform.PlatformProvider

val platform = PlatformProvider.createLocal("my-app")
val spark = platform.spark
val fetcher = platform.fetcher
val writer = platform.writer

// Use spark for your operations
platform.stop()
```

### Reading Data

```scala
import platform.PlatformProvider

val platform = PlatformProvider.createLocal("my-app")
val fetcher = platform.fetcher

// Read Parquet
val df = fetcher.readParquet(platform.spark, "path/to/data.parquet")

// Read from Iceberg table
val df = fetcher.readIcebergTable(platform.spark, "db.feature_table")

// Read CSV
val df = fetcher.readCsv(platform.spark, "path/to/data.csv", header = true)
```

### Writing Data

```scala
import platform.PlatformProvider

val platform = PlatformProvider.createLocal("my-app")
val writer = platform.writer

// Write Parquet
writer.writeParquet(df, "path/to/output", mode = "overwrite")

// Write partitioned Parquet
writer.writeParquet(df, "path/to/output", partitionBy = Seq("date", "region"))

// Write to Iceberg table
writer.insertOverwriteIcebergTable(df, "db.feature_table", partitionBy = Seq("date"))

// Write JSON/CSV
writer.writeJson(df, "path/to/output.json")
writer.writeCsv(df, "path/to/output.csv", header = true, delimiter = ",")
```

### Testing

Extend `SparkTestBase` in your test classes. This provides a Spark session, in-memory `TestFetcher`, and `TestWriter`:

```scala
import com.example.featurestore.suit.SparkTestBase
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class MyTest extends AnyFunSuite with SparkTestBase with Matchers {
  test("E2E test with in-memory storage") {
    import spark.implicits._

    // ARRANGE
    val df = Seq(1, 2, 3).toDF("value")

    // ACTION
    testWriter.writeParquet(df, "test/path")
    val stored = testFetcher.getStoredData("test/path")

    // ASSERT
    stored should be(defined)
    stored.get.count() should be(3)
  }

  test("Iceberg table operations") {
    import spark.implicits._

    val df = Seq(("user1", 0.5), ("user2", 0.8)).toDF("user_id", "score")
    testWriter.insertOverwriteIcebergTable(df, "test_db.features")
    val stored = testFetcher.readIcebergTable(platform.spark, "test_db.features")
    stored.count() should be(2)
  }
}
```

**Key Points:**
- `TestWriter` stores DataFrames in memory (dictionary-based)
- `TestFetcher` reads from `TestWriter`'s in-memory storage
- All data operations happen in memory for fast, isolated tests
- Follow Arrange-Action-Assert pattern for E2E tests

## Key Principles

1. **Platform Abstraction**: Use `SparkPlatformTrait` to abstract Spark session, fetcher, and writer
2. **Encapsulation**: `Fetcher` and `Writer` are encapsulated in the platform
3. **Type Safety**: Use case classes and explicit schemas for data transformations
4. **Pure Functions**: All transformations should be pure with no side effects
5. **Testing**: All logic covered by E2E tests following Arrange-Action-Assert pattern
6. **In-Memory Testing**: `TestFetcher` and `TestWriter` provide fast, isolated tests without I/O

## Building for Deployment

```bash
make build
```

The assembled JAR is created in `target/scala-2.13/` and can be submitted to a Spark cluster using `spark-submit`.

