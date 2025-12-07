package com.example.featurestore.types

import java.sql.{Date, Timestamp}

/** Training data: labels joined with features.
  *
  * This is the result of point-in-time join, ensuring no data leakage. Combines label information
  * with feature snapshots at the label timestamp.
  */
case class TrainingData(
  user_id: String,
  label: Double,
  as_of_ts: Timestamp,
  day: Date,
  event_count_7d: Option[Long],
  event_count_30d: Option[Long],
  last_event_days_ago: Option[Int],
  event_type_counts: Option[String]
)
