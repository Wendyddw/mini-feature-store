package platform

import org.apache.spark.sql.SparkSession

/** Trait defining the interface for Spark platform implementations.
  *
  * Provides abstraction over platform creation, allowing different implementations (e.g.,
  * SparkPlatform for production, MockPlatform for testing) while maintaining a consistent
  * interface.
  *
  * Common use patterns:
  * {{{
  *   // Work with the trait interface (recommended)
  *   def processData(platform: SparkPlatformTrait): Unit = {
  *     val df = platform.fetcher.readParquet(platform.spark, "input.parquet")
  *     platform.writer.writeParquet(df, "output.parquet")
  *   }
  *
  *   // Use in production
  *   val prodPlatform = PlatformProvider.createLocal("prod-app")
  *   processData(prodPlatform)
  *
  *   // Use in tests
  *   val testPlatform = PlatformProvider.createLocal("test", writer = new TestWriter())
  *   processData(testPlatform)
  * }}}
  */
trait SparkPlatformTrait {

  /** The SparkSession for data processing operations */
  val spark: SparkSession

  /** The Fetchers implementation for data input operations */
  val fetcher: Fetchers

  /** The Writers implementation for data output operations */
  val writer: Writers

  /** Stops the platform and releases all resources */
  def stop(): Unit
}
