package com.example.featurestore.suit

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.StructType
import platform.Writers

/** In-memory implementation of Writers for testing.
  *
  * Stores DataFrames as materialized Row data with their schemas, making them
  * independent of the SparkSession lifecycle. This ensures test data remains
  * valid even if the SparkSession is recreated.
  *
  * @param spark SparkSession used to convert stored data back to DataFrames
  */
class TestWriter(spark: SparkSession) extends Writers {
  // Store materialized data and schema separately for better test isolation
  // Made protected so TestFetcher can access the storage
  // Using immutable Map with var for functional style (reassign on each write)
  protected[suit] var db: Map[String, Seq[Row]] = Map.empty
  protected[suit] var dbSchema: Map[String, StructType] = Map.empty

  /** Returns all storage keys that have been written to. */
  def getAllStoredKeys: Set[String] = db.keys.toSet

  /** Clears all stored data and schemas. */
  def clearStorage(): Unit = {
    db = Map.empty
    dbSchema = Map.empty
  }

  /** Stores DataFrame data by materializing it immediately.
    * This ensures data is independent of the original DataFrame's SparkSession.
    */
  private def storeDataFrame(key: String, df: DataFrame, mode: String): Unit = {
    // Materialize the DataFrame immediately to avoid lazy evaluation issues
    val rows = df.collect().toSeq
    val schema = df.schema

    mode match {
      case "overwrite" | "error" =>
        db = db + (key -> rows)
        dbSchema = dbSchema + (key -> schema)
      case "append" =>
        val existingRows = db.getOrElse(key, Seq.empty)
        db = db + (key -> (existingRows ++ rows))
        // Schema should match, but we'll keep the original
        if (!dbSchema.contains(key)) {
          dbSchema = dbSchema + (key -> schema)
        }
      case "ignore" =>
        if (!db.contains(key)) {
          db = db + (key -> rows)
          dbSchema = dbSchema + (key -> schema)
        }
      case _ =>
        db = db + (key -> rows)
        dbSchema = dbSchema + (key -> schema)
    }
  }

  override def writeParquet(
      df: DataFrame,
      path: String,
      mode: String = "overwrite",
      partitionBy: Seq[String] = Seq.empty
  ): Unit = {
    storeDataFrame(path, df, mode)
  }

  override def writeJson(
      df: DataFrame,
      path: String,
      mode: String = "overwrite"
  ): Unit = {
    storeDataFrame(path, df, mode)
  }

  override def writeCsv(
      df: DataFrame,
      path: String,
      mode: String = "overwrite",
      header: Boolean = true,
      delimiter: String = ","
  ): Unit = {
    storeDataFrame(path, df, mode)
  }

  override def insertOverwriteIcebergTable(
      df: DataFrame,
      tableName: String,
      partitionBy: Seq[String] = Seq.empty
  ): Unit = {
    storeDataFrame(tableName, df, "overwrite")
  }
}

