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
├── project/                     # SBT project configuration
├── src/
│   ├── main/
│   │   ├── scala/
│   │   │   ├── com/example/featurestore/  # Feature store pipelines and types
│   │   │   │   ├── App.scala             # Main application entry point
│   │   │   │   ├── domain/              # Data schemas
│   │   │   │   ├── pipelines/           # Backfill, Point-in-Time Join, Online Sync
│   │   │   │   └── types/               # Pipeline configs and data types
│   │   │   └── platform/                # Core Spark utilities (Fetchers, Writers, Platform)
│   │   └── resources/                   # log4j2.xml, log4j.properties
│   └── test/
│       ├── scala/                      # Pipeline and unit tests
│       └── resources/                   # Test logging configuration
├── .scalafmt.conf              # Code formatting
├── .scalafix.conf              # Linting rules
└── Makefile                    # Build and test commands

```

## Prerequisites

* **Java 17** (required for Spark 3.5.0 compatibility; Java 25+ has compatibility issues)
* **SBT 1.9.6+**
* **Python 3** with `pandas` and `pyarrow` (for sample data generation)
* **Docker** and **Docker Compose** (for MinIO and Redis services)

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

### Running Pipelines

The main application entry point is `com.example.featurestore.App`, which supports three pipeline modes:

```bash
# Backfill: events_raw → features_daily
sbt "runMain com.example.featurestore.App backfill \
  --events-raw-path file:///tmp/events_raw \
  --output-table feature_store.features_daily \
  --start-date 2025-12-01 \
  --end-date 2025-12-07"

# Point-in-time join: labels + features_daily → training_data
sbt "runMain com.example.featurestore.App point-in-time-join \
  --labels-path file:///tmp/labels \
  --features-table feature_store.features_daily \
  --output-path file:///tmp/training_data"

# Online sync: features_daily → Redis
sbt "runMain com.example.featurestore.App online-sync \
  --features-table feature_store.features_daily \
  --redis-host localhost \
  --redis-port 6379 \
  --hours-back 168"
```

**Note**: For local development, the app automatically uses `local[*]` mode. For production, set the `SPARK_MASTER` environment variable or use `spark-submit`.

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

## Configuration

### Iceberg and S3/MinIO Setup

The application is configured for local development with MinIO (S3-compatible storage) and Iceberg:

- **S3A Configuration**: Connects to MinIO at `http://localhost:9000` with credentials `minioadmin/minioadmin`
- **Iceberg Catalog**: Uses Hadoop catalog type (no Hive Metastore required) with warehouse at `s3a://warehouse/`
- **Configuration**: Managed in `App.scala` via `getSparkConfigForIceberg()` method

For production, update the S3 endpoint and credentials in `App.scala` or use environment variables.

### Logging

- **log4j2.xml**: Main logging configuration for Spark, Hadoop, and Parquet
- **log4j.properties**: Log4j 1.x configuration for AWS SDK compatibility (suppresses warnings)

## Building for Deployment

```bash
make build
```

The assembled JAR is created in `target/scala-2.13/mini-feature-store-spark.jar` and can be submitted to a Spark cluster using `spark-submit`:

```bash
spark-submit --class com.example.featurestore.App \
  mini-feature-store-spark.jar \
  backfill \
  --events-raw-path s3://bucket/events_raw \
  --output-table feature_store.features_daily \
  --start-date 2025-01-01 \
  --end-date 2025-12-31
```

