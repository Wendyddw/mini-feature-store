package com.example.featurestore.suit

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}
import platform.{PlatformProvider, SparkPlatform, SparkPlatformTrait}

trait SparkTestBase extends BeforeAndAfterAll { this: Suite =>

  @transient var platform: SparkPlatformTrait = _
  @transient var spark: SparkSession          = _
  @transient var testWriter: TestWriter       = _
  @transient var testFetcher: TestFetcher     = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    platform = PlatformProvider.createLocal(
      appName = this.getClass.getSimpleName,
      config = Map(
        "spark.sql.shuffle.partitions" -> "1",
        "spark.default.parallelism"    -> "1"
      )
    )
    spark = platform.spark

    // Set up Iceberg catalog for tests (using temp directory for warehouse)
    val icebergWarehouse = System.getProperty("java.io.tmpdir") + "/iceberg-warehouse"
    spark.conf.set("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.iceberg.type", "hadoop")
    spark.conf.set("spark.sql.catalog.iceberg.warehouse", icebergWarehouse)

    // Create TestWriter with the SparkSession so it can recreate DataFrames
    testWriter = new TestWriter(spark)
    // Create TestFetcher that reads from TestWriter's in-memory storage
    testFetcher = new TestFetcher(testWriter, spark)
    // Replace the platform's writer and fetcher with our test implementations
    platform = new SparkPlatform(spark, testFetcher, testWriter)
  }

  override def afterAll(): Unit = {
    if (testWriter != null) {
      testWriter.clearStorage()
      testWriter = null
    }
    if (testFetcher != null) {
      testFetcher = null
    }
    if (platform != null) {
      platform.stop()
      platform = null
      spark = null
    }
    super.afterAll()
  }
}
