package com.example.featurestore.domain

import org.apache.spark.sql.types._

/** Schemas for feature store tables.
  *
  * Defines the structure of events_raw, labels, and features_daily tables.
  */
object Schemas {

  /** Schema for events_raw table.
    *
    * Stores raw events with user_id, event_type, and timestamp.
    */
  val eventsRawSchema: StructType = StructType(
    Array(
      StructField("user_id", StringType, nullable = false),
      StructField("event_type", StringType, nullable = false),
      StructField("ts", TimestampType, nullable = false)
    )
  )

  /** Schema for labels table.
    *
    * Defines training rows with user_id, label, and as_of_ts (point-in-time).
    */
  val labelsSchema: StructType = StructType(
    Array(
      StructField("user_id", StringType, nullable = false),
      StructField("label", DoubleType, nullable = false),
      StructField("as_of_ts", TimestampType, nullable = false)
    )
  )

  /** Schema for features_daily table.
    *
    * Stores daily aggregated features per user.
    * Features include:
    * - event_count_7d: count of events in last 7 days
    * - event_count_30d: count of events in last 30 days
    * - last_event_days_ago: days since last event
    * - event_type_counts: counts by event type (as JSON string for simplicity)
    */
  val featuresDailySchema: StructType = StructType(
    Array(
      StructField("user_id", StringType, nullable = false),
      StructField("day", DateType, nullable = false),
      StructField("event_count_7d", LongType, nullable = true),
      StructField("event_count_30d", LongType, nullable = true),
      StructField("last_event_days_ago", IntegerType, nullable = true),
      StructField("event_type_counts", StringType, nullable = true) // JSON string
    )
  )
}

