# Spark Framework for Mini Feature Store

This directory contains the Spark and Scala framework setup for the mini feature store project.

## Features

* **Platform Abstraction**: Clean separation between Spark platform creation and business logic
* **Type-Safe Utilities**: Fetchers and Writers for common data operations, both encapsulated in platform
* **Test Framework**: Base trait for Spark testing with in-memory TestFetcher and TestWriter
* **E2E Testing**: Proper end-to-end tests following Arrange-Action-Assert pattern
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

* Java 17 (required for Spark 3.5.0 compatibility)
* SBT 1.9.6+
* Java 17 is automatically configured for tests via `build.sbt`

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

// Create a local Spark platform (includes Spark session, fetcher, and writer)
val platform = PlatformProvider.createLocal("my-app")
val spark = platform.spark
val fetcher = platform.fetcher
val writer = platform.writer

// Use spark for your operations
// ...

platform.stop()
```

### Reading Data

```scala
import platform.PlatformProvider

// Create platform (includes fetcher)
val platform = PlatformProvider.createLocal("my-app")
val fetcher = platform.fetcher

// Read Parquet with optional schema
val df = fetcher.readParquet(platform.spark, "path/to/data.parquet")

// Read from Iceberg table
val df = fetcher.readIcebergTable(platform.spark, "db.feature_table")

// Read CSV
val df = fetcher.readCsv(platform.spark, "path/to/data.csv", header = true)
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

// Or use custom fetcher and writer
import platform.{ProdFetcher, ProdWriter}
val customFetcher = ProdFetcher
val customWriter = new ProdWriter()
val platformWithCustom = PlatformProvider.createLocal("my-app", fetcher = customFetcher, writer = customWriter)
```

### Testing

Extend `SparkTestBase` in your test classes. This provides a Spark session, in-memory `TestFetcher`, and `TestWriter`:

```scala
import com.example.featurestore.suit.SparkTestBase
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class MyTest extends AnyFunSuite with SparkTestBase with Matchers {
  test("E2E test with in-memory storage") {
    val sparkSession = spark
    import sparkSession.implicits._

    // ARRANGE: Create test data
    val df = Seq(1, 2, 3).toDF("value")

    // Write to in-memory storage (no actual I/O)
    // testWriter and testFetcher are automatically available from SparkTestBase
    testWriter.writeParquet(df, "test/path")

    // ACTION: Read back using testFetcher
    val stored = testFetcher.getStoredData("test/path")

    // ASSERT: Verify results
    stored should be(defined)
    stored.get.count() should be(3)

    // Check all stored keys
    testWriter.getAllStoredKeys should contain("test/path")

    // You can also use platform.fetcher and platform.writer (same instances)
    platform.writer.writeJson(df, "test/json")
    val jsonData = platform.fetcher.readJson(platform.spark, "test/json")
    jsonData.count() should be(3)
  }

  test("test Iceberg table operations") {
    val sparkSession = spark
    import sparkSession.implicits._

    // ARRANGE
    val df = Seq(("user1", 0.5), ("user2", 0.8)).toDF("user_id", "score")

    // ACTION
    testWriter.insertOverwriteIcebergTable(df, "test_db.features")
    val stored = testFetcher.readIcebergTable(platform.spark, "test_db.features")

    // ASSERT
    stored.count() should be(2)
  }
}
```

**Key Points:**
- `TestWriter` stores all DataFrames in memory (dictionary-based storage)
- `TestFetcher` reads from `TestWriter`'s in-memory storage
- All data operations happen in memory, making tests fast and isolated
- Follow Arrange-Action-Assert pattern for E2E tests
- Use `testFetcher.getStoredData()` to retrieve stored data for assertions

## Key Principles

1. **Platform Abstraction**: Use `SparkPlatformTrait` to abstract Spark session, fetcher, and writer
2. **Encapsulation**: Both `Fetcher` and `Writer` are encapsulated in the platform (like `platform.fetcher`, `platform.writer`)
3. **Type Safety**: Use case classes and explicit schemas for data transformations
4. **Pure Functions**: All transformations should be pure with no side effects
5. **Testing**: All logic should be covered by E2E tests following Arrange-Action-Assert pattern
6. **In-Memory Testing**: `TestFetcher` and `TestWriter` provide fast, isolated tests without I/O

## Building for Deployment

Create a fat JAR for cluster deployment:

```bash
make build
```

The assembled JAR will be created in `target/scala-2.13/` and can be submitted to a Spark cluster using `spark-submit`.

