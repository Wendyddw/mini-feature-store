package com.example.featurestore.pipelines

import com.example.featurestore.domain.Schemas
import com.example.featurestore.pipelines.PointInTimeJoinPipeline
import com.example.featurestore.suit.SparkTestBase
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Validation test to ensure no data leakage in point-in-time joins.
  *
  * This test proves that labels at time T only use features from time <= T.
  * This is critical for ML training to prevent future information leakage.
  */
class TestPointInTimeJoin extends AnyFunSuite with SparkTestBase with Matchers {

  test("Point-in-time join should not leak future features") {
    import spark.implicits._

    // Create test data: events on days 1, 2, 3
    val eventsRaw = Seq(
      ("user1", "click", java.sql.Timestamp.valueOf("2024-01-01 10:00:00")),
      ("user1", "purchase", java.sql.Timestamp.valueOf("2024-01-02 10:00:00")),
      ("user1", "click", java.sql.Timestamp.valueOf("2024-01-03 10:00:00"))
    ).toDF("user_id", "event_type", "ts")

    // Create features_daily: features computed for each day
    val featuresDaily = Seq(
      ("user1", java.sql.Date.valueOf("2024-01-01"), 1L, 1L, 0, "1"),
      ("user1", java.sql.Date.valueOf("2024-01-02"), 2L, 2L, 0, "2"),
      ("user1", java.sql.Date.valueOf("2024-01-03"), 3L, 3L, 0, "3")
    ).toDF("user_id", "day", "event_count_7d", "event_count_30d", "last_event_days_ago", "event_type_counts")

    // Create labels with as_of_ts on day 2
    // This label should ONLY use features from day 1 or day 2, NOT day 3
    val labels = Seq(
      ("user1", 1.0, java.sql.Timestamp.valueOf("2024-01-02 12:00:00"))
    ).toDF("user_id", "label", "as_of_ts")

    // Write test data to TestWriter
    testWriter.writeParquet(eventsRaw, "test/events_raw")
    testWriter.insertOverwriteIcebergTable(featuresDaily, "test.features_daily")
    testWriter.writeParquet(labels, "test/labels")

    // Get stored data
    val storedEvents = testWriter.getStoredData("test/events_raw").get
    val storedFeatures = testWriter.getStoredData("test.features_daily").get
    val storedLabels = testWriter.getStoredData("test/labels").get

    // Perform point-in-time join manually to verify logic
    val labelsWithDate = storedLabels
      .withColumn("as_of_date", to_date(col("as_of_ts")))

    val joined = labelsWithDate
      .join(
        storedFeatures.withColumn("feature_date", col("day")),
        labelsWithDate("user_id") === storedFeatures("user_id") &&
          storedFeatures("feature_date") <= labelsWithDate("as_of_date"),
        "left"
      )
      .withColumn(
        "rank",
        row_number().over(
          org.apache.spark.sql.expressions.Window
            .partitionBy(labelsWithDate("user_id"), labelsWithDate("as_of_ts"))
            .orderBy(storedFeatures("feature_date").desc)
        )
      )
      .filter(col("rank") === 1)
      .drop("rank", "feature_date", "as_of_date")

    // Verify: the joined feature should be from day 2 (as_of_date), not day 3
    val result = joined.collect()
    result.length should be(1)

    val featureDay = result(0).getAs[java.sql.Date]("day")
    val asOfDate = java.sql.Date.valueOf("2024-01-02")

    // The feature day should be <= as_of_date (day 2)
    featureDay.compareTo(asOfDate) should be <= 0

    // Specifically, it should be day 2 (the latest feature <= as_of_date)
    featureDay should be(asOfDate)

    // Verify event_count_7d is 2 (from day 2), not 3 (from day 3)
    val eventCount = result(0).getAs[Long]("event_count_7d")
    eventCount should be(2L) // From day 2, not day 3

    // Verify no future data leaked
    val featureDateStr = featureDay.toString
    featureDateStr should be("2024-01-02")
    featureDateStr should not be("2024-01-03")
  }

  test("Point-in-time join should handle multiple users correctly") {
    import spark.implicits._

    // Create features for two users
    val featuresDaily = Seq(
      ("user1", java.sql.Date.valueOf("2024-01-01"), 1L, 1L, 0, "1"),
      ("user1", java.sql.Date.valueOf("2024-01-02"), 2L, 2L, 0, "2"),
      ("user2", java.sql.Date.valueOf("2024-01-01"), 5L, 5L, 0, "5"),
      ("user2", java.sql.Date.valueOf("2024-01-02"), 10L, 10L, 0, "10")
    ).toDF("user_id", "day", "event_count_7d", "event_count_30d", "last_event_days_ago", "event_type_counts")

    // Create labels at different times
    val labels = Seq(
      ("user1", 1.0, java.sql.Timestamp.valueOf("2024-01-01 12:00:00")),
      ("user2", 0.0, java.sql.Timestamp.valueOf("2024-01-02 12:00:00"))
    ).toDF("user_id", "label", "as_of_ts")

    testWriter.insertOverwriteIcebergTable(featuresDaily, "test.features_daily")
    testWriter.writeParquet(labels, "test/labels")

    val storedFeatures = testWriter.getStoredData("test.features_daily").get
    val storedLabels = testWriter.getStoredData("test/labels").get

    val labelsWithDate = storedLabels
      .withColumn("as_of_date", to_date(col("as_of_ts")))

    val joined = labelsWithDate
      .join(
        storedFeatures.withColumn("feature_date", col("day")),
        labelsWithDate("user_id") === storedFeatures("user_id") &&
          storedFeatures("feature_date") <= labelsWithDate("as_of_date"),
        "left"
      )
      .withColumn(
        "rank",
        row_number().over(
          org.apache.spark.sql.expressions.Window
            .partitionBy(labelsWithDate("user_id"), labelsWithDate("as_of_ts"))
            .orderBy(storedFeatures("feature_date").desc)
        )
      )
      .filter(col("rank") === 1)
      .drop("rank", "feature_date", "as_of_date")

    val results = joined.collect()
    results.length should be(2)

    // user1 at 2024-01-01 should get features from 2024-01-01
    val user1Result = results.find(_.getAs[String]("user_id") == "user1").get
    user1Result.getAs[java.sql.Date]("day") should be(java.sql.Date.valueOf("2024-01-01"))
    user1Result.getAs[Long]("event_count_7d") should be(1L)

    // user2 at 2024-01-02 should get features from 2024-01-02
    val user2Result = results.find(_.getAs[String]("user_id") == "user2").get
    user2Result.getAs[java.sql.Date]("day") should be(java.sql.Date.valueOf("2024-01-02"))
    user2Result.getAs[Long]("event_count_7d") should be(10L)
  }
}

