package platform

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

object Fetchers {
  def readParquet(
      spark: SparkSession,
      path: String,
      schema: Option[StructType] = None
  ): DataFrame = {
    val reader = spark.read
    val readerWithSchema = schema match {
      case Some(s) => reader.schema(s)
      case None    => reader
    }
    readerWithSchema.parquet(path)
  }

  def readJson(
      spark: SparkSession,
      path: String,
      schema: Option[StructType] = None
  ): DataFrame = {
    val reader = spark.read
    val readerWithSchema = schema match {
      case Some(s) => reader.schema(s)
      case None    => reader
    }
    readerWithSchema.json(path)
  }

  def readCsv(
      spark: SparkSession,
      path: String,
      schema: Option[StructType] = None,
      header: Boolean = true,
      delimiter: String = ","
  ): DataFrame = {
    val reader = spark.read
      .option("header", header)
      .option("delimiter", delimiter)
    val readerWithSchema = schema match {
      case Some(s) => reader.schema(s)
      case None    => reader
    }
    readerWithSchema.csv(path)
  }

  def readIcebergTable(
      spark: SparkSession,
      tableName: String
  ): DataFrame = {
    spark.read
      .format("iceberg")
      .table(tableName)
  }
}

