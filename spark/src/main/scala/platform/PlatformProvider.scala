package platform

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/** Factory object for creating Spark platform instances.
  *
  * Provides convenient methods to create configured SparkPlatform instances
  * with SparkSession and Writers. This follows the Factory Pattern to abstract
  * platform creation logic.
  *
  * Common use patterns:
  * {{{
  *   // Local development/testing
  *   val platform = PlatformProvider.createLocal("my-app")
  *   val df = platform.spark.read.parquet("data.parquet")
  *   platform.writer.writeParquet(df, "output.parquet")
  *   platform.stop()
  *
  *   // Production cluster deployment
  *   val platform = PlatformProvider.create(
  *     appName = "feature-pipeline",
  *     master = Some("yarn"),
  *     config = Map("spark.sql.shuffle.partitions" -> "200")
  *   )
  *
  *   // Custom writer for testing
  *   val testWriter = new TestWriter()
  *   val platform = PlatformProvider.createLocal("test-app", writer = testWriter)
  * }}}
  */
object PlatformProvider {
  /** Creates a Spark platform with configurable master and settings.
    *
    * @param appName Name of the Spark application (appears in Spark UI)
    * @param master Spark master URL (e.g., "yarn", "local[*]", "spark://host:port").
    *               If None, uses default from spark-submit or spark-defaults.conf
    * @param config Additional Spark configuration options as key-value pairs
    * @param fetcher Fetcher implementation to use (defaults to ProdFetcher for production)
    * @param writer Writer implementation to use (defaults to ProdWriter for production)
    * @return Configured SparkPlatformTrait instance with SparkSession, Fetcher, and Writer
    *
    * @example
    * {{{
    *   // Cluster deployment
    *   val platform = PlatformProvider.create(
    *     appName = "feature-store-pipeline",
    *     master = Some("yarn"),
    *     config = Map(
    *       "spark.sql.shuffle.partitions" -> "200",
    *       "spark.executor.memory" -> "4g"
    *     )
    *   )
    *
    *   // Using existing SparkSession (when submitted via spark-submit)
    *   val platform = PlatformProvider.create(
    *     appName = "my-job",
    *     master = None  // Uses cluster manager from spark-submit
    *   )
    * }}}
    */
  def create(
      appName: String,
      master: Option[String] = None,
      config: Map[String, String] = Map.empty,
      fetcher: Fetchers = ProdFetcher,
      writer: Writers = new ProdWriter()
  ): SparkPlatformTrait = {
    val sparkConf = new SparkConf()
      .setAppName(appName)
      .setAll(config)

    master.foreach(sparkConf.setMaster)

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    new SparkPlatform(spark, fetcher, writer)
  }

  /** Creates a local Spark platform for development and testing.
    *
    * Sets master to "local[*]" to use all available CPU cores locally.
    * This is ideal for local development, unit testing, and debugging.
    *
    * @param appName Name of the Spark application
    * @param config Additional Spark configuration (commonly used for testing)
    * @param fetcher Fetcher implementation (use TestFetcher for in-memory testing)
    * @param writer Writer implementation (use TestWriter for in-memory testing)
    * @return Configured SparkPlatformTrait instance for local execution
    *
    * @example
    * {{{
    *   // Basic local usage
    *   val platform = PlatformProvider.createLocal("my-local-app")
    *   import platform.spark.implicits._
    *   val df = Seq(1, 2, 3).toDF("value")
    *   platform.writer.writeParquet(df, "output.parquet")
    *   platform.stop()
    *
    *   // Local testing with custom config
    *   val platform = PlatformProvider.createLocal(
    *     appName = "test-pipeline",
    *     config = Map(
    *       "spark.sql.shuffle.partitions" -> "1",
    *       "spark.default.parallelism" -> "1"
    *     )
    *   )
    *
    *   // Local testing with in-memory writer
    *   val testWriter = new TestWriter()
    *   val platform = PlatformProvider.createLocal("test", writer = testWriter)
    *   platform.writer.writeParquet(df, "test/path")
    *   val stored = testWriter.getStoredData("test/path")  // Retrieve for assertions
    * }}}
    */
  def createLocal(
      appName: String,
      config: Map[String, String] = Map.empty,
      fetcher: Fetchers = ProdFetcher,
      writer: Writers = new ProdWriter()
  ): SparkPlatformTrait = {
    create(appName, Some("local[*]"), config, fetcher, writer)
  }
}

