package com.example.featurestore.types

import java.sql.Timestamp

/** Label from labels table.
  *
  * Represents a training label with user_id, label value, and point-in-time timestamp.
  */
case class Label(
    user_id: String,
    label: Double,
    as_of_ts: Timestamp
)

