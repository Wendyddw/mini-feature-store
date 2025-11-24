package com.example.featurestore.suit

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class TestWriterTest extends AnyFunSuite with SparkTestBase with Matchers {

  test("TestWriter should store DataFrames in memory") {
    import spark.implicits._

    val data = Seq(("Alice", 30), ("Bob", 25))
    val df = data.toDF("name", "age")

    testWriter.writeParquet(df, "test/path/data.parquet")

    val stored = testWriter.getStoredData("test/path/data.parquet")
    stored should be(defined)
    stored.get.count() should be(2)
    stored.get.columns should contain("name")
    stored.get.columns should contain("age")
  }

  test("TestWriter should support writeJson") {
    import spark.implicits._

    val data = Seq(("item1", 10), ("item2", 20))
    val df = data.toDF("item", "value")

    testWriter.writeJson(df, "test/json/data.json")

    val stored = testWriter.getStoredData("test/json/data.json")
    stored should be(defined)
    stored.get.count() should be(2)
  }

  test("TestWriter should support writeCsv") {
    import spark.implicits._

    val data = Seq(("A", 1), ("B", 2), ("C", 3))
    val df = data.toDF("letter", "number")

    testWriter.writeCsv(df, "test/csv/data.csv", header = true)

    val stored = testWriter.getStoredData("test/csv/data.csv")
    stored should be(defined)
    stored.get.count() should be(3)
  }

  test("TestWriter should support insertOverwriteIcebergTable") {
    import spark.implicits._

    val data = Seq(("user1", "feature1", 0.5), ("user2", "feature2", 0.8))
    val df = data.toDF("user_id", "feature_name", "value")

    testWriter.insertOverwriteIcebergTable(df, "test_db.feature_table")

    val stored = testWriter.getStoredData("test_db.feature_table")
    stored should be(defined)
    stored.get.count() should be(2)
    stored.get.columns should contain("user_id")
  }

  test("TestWriter should track all stored keys") {
    import spark.implicits._

    val df1 = Seq(1, 2, 3).toDF("value")
    val df2 = Seq("a", "b").toDF("letter")

    testWriter.writeParquet(df1, "path1")
    testWriter.writeJson(df2, "path2")

    val keys = testWriter.getAllStoredKeys
    keys should contain("path1")
    keys should contain("path2")
    keys.size should be(2)
  }

  test("TestWriter should clear storage") {
    import spark.implicits._

    val df = Seq(1, 2, 3).toDF("value")
    testWriter.writeParquet(df, "test/path")

    testWriter.getStoredData("test/path") should be(defined)

    testWriter.clearStorage()

    testWriter.getStoredData("test/path") should be(None)
    testWriter.getAllStoredKeys should be(empty)
  }
}

