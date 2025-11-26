package com.example.featurestore.pipelines

import com.example.featurestore.types.{
  PointInTimeJoinPipelineConfig,
  TrainingData
}
import com.example.featurestore.suit.SparkTestBase
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** End-to-end test for PointInTimeJoinPipeline.
  *
  * Tests the complete pipeline flow:
  * 1. Arrange: Create test data (features table and labels parquet)
  * 2. Action: Execute PointInTimeJoinPipeline
  * 3. Assert: Compare expected vs actual results
  *
  * This ensures no data leakage - labels at time T only use features from time <= T.
  */
class TestPointInTimeJoin extends AnyFunSuite with SparkTestBase with Matchers {

  test("Point-in-time join should not leak future features") {
    val sparkSession = spark
    import sparkSession.implicits._

    // ARRANGE: Create test data
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

    // Write test data using TestWriter
    testWriter.insertOverwriteIcebergTable(featuresDaily, "test.features_daily")
    testWriter.writeParquet(labels, "test/labels")

    // Expected result: label from day 2 should use features from day 2 (latest <= as_of_date)
    val expected = Seq(
      TrainingData(
        user_id = "user1",
        label = 1.0,
        as_of_ts = java.sql.Timestamp.valueOf("2024-01-02 12:00:00"),
        day = java.sql.Date.valueOf("2024-01-02"),
        event_count_7d = Some(2L),
        event_count_30d = Some(2L),
        last_event_days_ago = Some(0),
        event_type_counts = Some("2")
      )
    )

    // ACTION: Execute pipeline
    val config = PointInTimeJoinPipelineConfig(
      labelsPath = "test/labels",
      featuresTable = "test.features_daily",
      outputPath = "test/training_data"
    )
    val pipeline = new PointInTimeJoinPipeline(this.platform, config)
    val result = pipeline.execute()

    // ASSERT: Compare expected vs actual
    result should be(defined)
    val actual = result.get

    // Verify we got exactly one result
    actual.length should be(1)

    // Verify the result matches expected
    val actualResult = actual.head
    actualResult.user_id should be(expected.head.user_id)
    actualResult.label should be(expected.head.label)
    actualResult.as_of_ts should be(expected.head.as_of_ts)
    actualResult.day should be(expected.head.day)
    actualResult.event_count_7d should be(expected.head.event_count_7d)
    actualResult.event_count_30d should be(expected.head.event_count_30d)
    actualResult.last_event_days_ago should be(expected.head.last_event_days_ago)
    actualResult.event_type_counts should be(expected.head.event_type_counts)

    // Verify no future data leaked (day should be <= as_of_date)
    val asOfDate = java.sql.Date.valueOf("2024-01-02")
    actualResult.day.compareTo(asOfDate) should be <= 0

    // Specifically verify it's day 2, not day 3
    actualResult.day should be(asOfDate)
    actualResult.day.toString should be("2024-01-02")
    actualResult.day.toString should not be("2024-01-03")

    // Verify event_count_7d is 2 (from day 2), not 3 (from day 3)
    actualResult.event_count_7d should be(Some(2L))
    actualResult.event_count_7d should not be(Some(3L))

    // Verify output was written
    val outputData = testFetcher.getStoredData("test/training_data")
    outputData should be(defined)
    outputData.get.count() should be(1)
  }

  test("Point-in-time join should handle multiple users correctly") {
    val sparkSession = spark
    import sparkSession.implicits._

    // ARRANGE: Create test data
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

    // Write test data using TestWriter
    testWriter.insertOverwriteIcebergTable(featuresDaily, "test.features_daily")
    testWriter.writeParquet(labels, "test/labels")

    // Expected results
    val expected = Seq(
      TrainingData(
        user_id = "user1",
        label = 1.0,
        as_of_ts = java.sql.Timestamp.valueOf("2024-01-01 12:00:00"),
        day = java.sql.Date.valueOf("2024-01-01"),
        event_count_7d = Some(1L),
        event_count_30d = Some(1L),
        last_event_days_ago = Some(0),
        event_type_counts = Some("1")
      ),
      TrainingData(
        user_id = "user2",
        label = 0.0,
        as_of_ts = java.sql.Timestamp.valueOf("2024-01-02 12:00:00"),
        day = java.sql.Date.valueOf("2024-01-02"),
        event_count_7d = Some(10L),
        event_count_30d = Some(10L),
        last_event_days_ago = Some(0),
        event_type_counts = Some("10")
      )
    )

    // ACTION: Execute pipeline
    val config = PointInTimeJoinPipelineConfig(
      labelsPath = "test/labels",
      featuresTable = "test.features_daily",
      outputPath = "test/training_data"
    )
    val pipeline = new PointInTimeJoinPipeline(this.platform, config)
    val result = pipeline.execute()

    // ASSERT: Compare expected vs actual
    result should be(defined)
    val actual = result.get

    // Verify we got exactly two results
    actual.length should be(2)

    // Verify user1 result
    val user1Result = actual.find(_.user_id == "user1").get
    user1Result.day should be(expected.find(_.user_id == "user1").get.day)
    user1Result.event_count_7d should be(expected.find(_.user_id == "user1").get.event_count_7d)
    user1Result.label should be(1.0)

    // Verify user2 result
    val user2Result = actual.find(_.user_id == "user2").get
    user2Result.day should be(expected.find(_.user_id == "user2").get.day)
    user2Result.event_count_7d should be(expected.find(_.user_id == "user2").get.event_count_7d)
    user2Result.label should be(0.0)

    // Verify output was written
    val outputData = testFetcher.getStoredData("test/training_data")
    outputData should be(defined)
    outputData.get.count() should be(2)
  }
}
