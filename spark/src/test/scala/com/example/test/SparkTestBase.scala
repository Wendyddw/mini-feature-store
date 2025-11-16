package com.example.test

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}
import platform.PlatformProvider

trait SparkTestBase extends BeforeAndAfterAll { this: Suite =>

  @transient var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val platform = PlatformProvider.createLocal(
      appName = this.getClass.getSimpleName,
      config = Map(
        "spark.sql.shuffle.partitions" -> "1",
        "spark.default.parallelism" -> "1"
      )
    )
    spark = platform.spark
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
      spark = null
    }
    super.afterAll()
  }
}

