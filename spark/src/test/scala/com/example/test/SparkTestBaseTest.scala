package com.example.test

import org.apache.spark.sql.DataFrame
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SparkTestBaseTest extends AnyFunSuite with SparkTestBase with Matchers {

  test("Spark session should be available") {
    spark should not be null
    spark.version should not be empty
  }

  test("Should be able to create DataFrames") {
    import spark.implicits._

    val data = Seq(("Alice", 30), ("Bob", 25), ("Charlie", 35))
    val df: DataFrame = data.toDF("name", "age")

    df.count() should be(3)
    df.columns should contain("name")
    df.columns should contain("age")
  }

  test("Should be able to perform transformations") {
    import spark.implicits._

    val data = Seq(1, 2, 3, 4, 5)
    val df = data.toDF("value")

    val result = df.filter($"value" > 2)

    result.count() should be(3)
  }
}

