package com.example.featurestore.pipelines

import com.example.featurestore.types.{BackfillPipelineConfig, FeaturesDaily}
import com.example.featurestore.suit.SparkTestBase
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** End-to-end test for BackfillPipeline.
  *
  * Tests the complete pipeline flow:
  *   1. Arrange: Create test data (events_raw parquet) 2. Action: Execute BackfillPipeline 3.
  *      Assert: Compare expected vs actual results
  *
  * This ensures complete coverage (every user has a row for every day) and correct rolling window
  * calculations.
  */
class TestBackfillPipeline extends AnyFunSuite with SparkTestBase with Matchers {

  test("Backfill should create features for every day in date range") {
    val sparkSession = spark
    import sparkSession.implicits._

    // ARRANGE: Create test data
    // Create events_raw: events on day 1, 3, and 5
    val eventsRaw = Seq(
      ("user1", "click", java.sql.Timestamp.valueOf("2024-01-01 10:00:00")),
      ("user1", "purchase", java.sql.Timestamp.valueOf("2024-01-03 14:30:00")),
      ("user1", "click", java.sql.Timestamp.valueOf("2024-01-05 16:45:00"))
    ).toDF("user_id", "event_type", "ts")

    // Write test data using TestWriter
    testWriter.writeParquet(eventsRaw, "test/events_raw")

    // Expected results: every day from 2024-01-01 to 2024-01-05 should have a row
    // Day 1: 1 event (click) → 1 in 7d/30d, 0 days ago, 1 distinct type
    // Day 2: 1 event (click from day 1) → 1 in 7d/30d, 1 day ago, 1 distinct type
    // Day 3: 2 events (click from day 1, purchase from day 3) → 2 in 7d/30d, 0 days ago, 2 distinct types
    // Day 4: 2 events (click from day 1, purchase from day 3) → 2 in 7d/30d, 1 day ago, 2 distinct types
    // Day 5: 3 events (all three) → 3 in 7d/30d, 0 days ago, 2 distinct types
    val expected = Seq(
      FeaturesDaily(
        user_id = "user1",
        day = java.sql.Date.valueOf("2024-01-01"),
        event_count_7d = Some(1L),
        event_count_30d = Some(1L),
        last_event_days_ago = Some(0),
        event_type_counts = Some("1")
      ),
      FeaturesDaily(
        user_id = "user1",
        day = java.sql.Date.valueOf("2024-01-02"),
        event_count_7d = Some(1L),
        event_count_30d = Some(1L),
        last_event_days_ago = Some(1),
        event_type_counts = Some("1")
      ),
      FeaturesDaily(
        user_id = "user1",
        day = java.sql.Date.valueOf("2024-01-03"),
        event_count_7d = Some(2L),
        event_count_30d = Some(2L),
        last_event_days_ago = Some(0),
        event_type_counts = Some("2")
      ),
      FeaturesDaily(
        user_id = "user1",
        day = java.sql.Date.valueOf("2024-01-04"),
        event_count_7d = Some(2L),
        event_count_30d = Some(2L),
        last_event_days_ago = Some(1),
        event_type_counts = Some("2")
      ),
      FeaturesDaily(
        user_id = "user1",
        day = java.sql.Date.valueOf("2024-01-05"),
        event_count_7d = Some(3L),
        event_count_30d = Some(3L),
        last_event_days_ago = Some(0),
        event_type_counts = Some("2")
      )
    )

    // ACTION: Execute pipeline
    val config = BackfillPipelineConfig(
      eventsRawPath = "test/events_raw",
      outputTable = "test.features_daily",
      startDate = "2024-01-01",
      endDate = "2024-01-05"
    )
    val pipeline = new BackfillPipeline(this.platform, config)
    val result   = pipeline.execute()

    // ASSERT: Pipeline returns None (writes to table, doesn't return data)
    result should be(None)

    // Verify output was written to Iceberg table
    val outputData = testFetcher.getStoredData("test.features_daily")
    outputData should be(defined)

    val actual = outputData.get.as[FeaturesDaily].collect().toSeq.sortBy(_.day.toString)

    // Verify we got exactly 5 results (one for each day)
    actual.length should be(5)

    // Verify each day's features match expected
    actual.zip(expected).foreach { case (actualRow, expectedRow) =>
      actualRow.user_id should be(expectedRow.user_id)
      actualRow.day should be(expectedRow.day)
      actualRow.event_count_7d should be(expectedRow.event_count_7d)
      actualRow.event_count_30d should be(expectedRow.event_count_30d)
      actualRow.last_event_days_ago should be(expectedRow.last_event_days_ago)
      actualRow.event_type_counts should be(expectedRow.event_type_counts)
    }

    // Verify complete coverage: day 2 and day 4 have rows even though no events occurred on those days
    val day2 = actual.find(_.day.toString == "2024-01-02").get
    day2.event_count_7d should be(Some(1L))     // Counts event from day 1
    day2.last_event_days_ago should be(Some(1)) // Last event was 1 day ago

    val day4 = actual.find(_.day.toString == "2024-01-04").get
    day4.event_count_7d should be(Some(2L))     // Counts events from day 1 and 3
    day4.last_event_days_ago should be(Some(1)) // Last event was 1 day ago (day 3)
  }

  test("Backfill should handle multiple users correctly") {
    val sparkSession = spark
    import sparkSession.implicits._

    // ARRANGE: Create test data for two users
    val eventsRaw = Seq(
      ("user1", "click", java.sql.Timestamp.valueOf("2024-01-01 10:00:00")),
      ("user1", "purchase", java.sql.Timestamp.valueOf("2024-01-02 14:30:00")),
      ("user2", "view", java.sql.Timestamp.valueOf("2024-01-01 09:00:00")),
      ("user2", "click", java.sql.Timestamp.valueOf("2024-01-02 11:00:00"))
    ).toDF("user_id", "event_type", "ts")

    // Write test data using TestWriter
    testWriter.writeParquet(eventsRaw, "test/events_raw")

    // ACTION: Execute pipeline
    val config = BackfillPipelineConfig(
      eventsRawPath = "test/events_raw",
      outputTable = "test.features_daily",
      startDate = "2024-01-01",
      endDate = "2024-01-02"
    )
    val pipeline = new BackfillPipeline(this.platform, config)
    val result   = pipeline.execute()

    // ASSERT: Pipeline returns None
    result should be(None)

    // Verify output was written
    val outputData = testFetcher.getStoredData("test.features_daily")
    outputData should be(defined)

    val actual = outputData.get.as[FeaturesDaily].collect().toSeq

    // Verify we got exactly 4 results (2 users × 2 days)
    actual.length should be(4)

    // Verify user1 results
    val user1Day1 = actual.find(r => r.user_id == "user1" && r.day.toString == "2024-01-01").get
    user1Day1.event_count_7d should be(Some(1L))
    user1Day1.event_type_counts should be(Some("1"))

    val user1Day2 = actual.find(r => r.user_id == "user1" && r.day.toString == "2024-01-02").get
    user1Day2.event_count_7d should be(Some(2L))     // Both events from day 1 and 2
    user1Day2.event_type_counts should be(Some("2")) // Two distinct types: click, purchase

    // Verify user2 results
    val user2Day1 = actual.find(r => r.user_id == "user2" && r.day.toString == "2024-01-01").get
    user2Day1.event_count_7d should be(Some(1L))
    user2Day1.event_type_counts should be(Some("1"))

    val user2Day2 = actual.find(r => r.user_id == "user2" && r.day.toString == "2024-01-02").get
    user2Day2.event_count_7d should be(Some(2L))     // Both events from day 1 and 2
    user2Day2.event_type_counts should be(Some("2")) // Two distinct types: view, click
  }

  test("Backfill should calculate rolling windows correctly") {
    val sparkSession = spark
    import sparkSession.implicits._

    // ARRANGE: Create events spread across 10 days to test 7-day and 30-day windows
    val eventsRaw = Seq(
      ("user1", "click", java.sql.Timestamp.valueOf("2024-01-01 10:00:00")),
      ("user1", "click", java.sql.Timestamp.valueOf("2024-01-05 14:00:00")),
      ("user1", "purchase", java.sql.Timestamp.valueOf("2024-01-08 16:00:00"))
    ).toDF("user_id", "event_type", "ts")

    // Write test data using TestWriter
    testWriter.writeParquet(eventsRaw, "test/events_raw")

    // ACTION: Execute pipeline
    val config = BackfillPipelineConfig(
      eventsRawPath = "test/events_raw",
      outputTable = "test.features_daily",
      startDate = "2024-01-01",
      endDate = "2024-01-10"
    )
    val pipeline = new BackfillPipeline(this.platform, config)
    val result   = pipeline.execute()

    // ASSERT: Pipeline returns None
    result should be(None)

    // Verify output was written
    val outputData = testFetcher.getStoredData("test.features_daily")
    outputData should be(defined)

    val actual = outputData.get.as[FeaturesDaily].collect().toSeq.sortBy(_.day.toString)

    // Verify day 8: should have 3 events in 7-day window (day 1, 5, and 8 all within 7 days), 3 events in 30-day window
    val day8 = actual.find(_.day.toString == "2024-01-08").get
    day8.event_count_7d should be(
      Some(3L)
    ) // All 3 events (day 1 is 7 days ago, day 5 is 3 days ago, day 8 is 0 days ago)
    day8.event_count_30d should be(Some(3L))    // All 3 events (within 30 days)
    day8.last_event_days_ago should be(Some(0)) // Event on same day
    day8.event_type_counts should be(Some("2")) // Two distinct types: click, purchase

    // Verify day 9: should have 2 events in 7-day window (day 5 and 8), 3 events in 30-day window
    val day9 = actual.find(_.day.toString == "2024-01-09").get
    day9.event_count_7d should be(Some(2L))  // Events from day 5 (4 days ago) and day 8 (1 day ago)
    day9.event_count_30d should be(Some(3L)) // All 3 events (within 30 days)
    day9.last_event_days_ago should be(Some(1)) // Last event was 1 day ago

    // Verify day 10: should have 2 events in 7-day window (day 5 and day 8), 3 events in 30-day window
    val day10 = actual.find(_.day.toString == "2024-01-10").get
    day10.event_count_7d should be(
      Some(2L)
    ) // Events from day 5 (5 days ago) and day 8 (2 days ago)
    day10.event_count_30d should be(Some(3L))    // All 3 events (within 30 days)
    day10.last_event_days_ago should be(Some(2)) // Last event was 2 days ago
  }
}
