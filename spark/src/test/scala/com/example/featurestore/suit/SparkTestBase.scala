package com.example.featurestore.suit

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}
import platform.{PlatformProvider, SparkPlatformTrait}

trait SparkTestBase extends BeforeAndAfterAll { this: Suite =>

  @transient var platform: SparkPlatformTrait = _
  @transient var spark: SparkSession = _
  @transient var testWriter: TestWriter = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    platform = PlatformProvider.createLocal(
      appName = this.getClass.getSimpleName,
      config = Map(
        "spark.sql.shuffle.partitions" -> "1",
        "spark.default.parallelism" -> "1"
      )
    )
    spark = platform.spark
    // Create TestWriter with the SparkSession so it can recreate DataFrames
    testWriter = new TestWriter(spark)
    // Replace the platform's writer with our test writer
    platform = new platform.SparkPlatform(spark, testWriter)
  }

  override def afterAll(): Unit = {
    if (testWriter != null) {
      testWriter.clearStorage()
      testWriter = null
    }
    if (platform != null) {
      platform.stop()
      platform = null
      spark = null
    }
    super.afterAll()
  }
}

