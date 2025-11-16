package platform

import org.apache.spark.sql.{DataFrame, SparkSession}

trait SparkPlatformTrait {
  def spark: SparkSession
  def stop(): Unit
}

