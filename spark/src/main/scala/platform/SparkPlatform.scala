package platform

import org.apache.spark.sql.SparkSession

/** Concrete implementation of SparkPlatformTrait.
  *
  * Encapsulates a SparkSession and Writer together, providing a unified
  * platform for data processing operations. The platform manages the lifecycle
  * of the Spark session.
  *
  * Common use patterns:
  * {{{
  *   // Platform is typically created via PlatformProvider
  *   val platform = PlatformProvider.createLocal("my-app")
  *
  *   // Access Spark session for data operations
  *   val df = platform.spark.read.parquet("input.parquet")
  *   val result = df.filter($"value" > 100)
  *
  *   // Use writer for output operations
  *   platform.writer.writeParquet(result, "output.parquet")
  *
  *   // Always stop the platform when done
  *   platform.stop()
  * }}}
  *
  * @param spark The SparkSession instance for data processing
  * @param writer The Writers implementation for data output operations
  */
class SparkPlatform(
    val spark: SparkSession,
    val writer: Writers
) extends SparkPlatformTrait {

  /** Stops the Spark session and releases resources.
    *
    * Always call this method when you're done with the platform to ensure
    * proper cleanup of Spark resources. In production, this is typically
    * done in a finally block or using resource management.
    *
    * @example
    * {{{
    *   val platform = PlatformProvider.createLocal("my-app")
    *   try {
    *     // Your processing logic
    *     val df = platform.spark.read.parquet("data.parquet")
    *     platform.writer.writeParquet(df, "output.parquet")
    *   } finally {
    *     platform.stop()
    *   }
    * }}}
    */
  def stop(): Unit = {
    spark.stop()
  }
}

