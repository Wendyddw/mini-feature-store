package platform

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

/** Trait defining the interface for data reading operations.
  *
  * Provides abstraction for reading DataFrames from various storage formats
  * and systems. Implementations include ProdFetcher (real storage) and
  * TestFetcher (in-memory for testing).
  */
trait Fetchers {
  /** Reads a DataFrame from Parquet format.
    *
    * @param spark SparkSession for reading
    * @param path Path to Parquet files
    * @param schema Optional schema to apply (default: None, infer from data)
    */
  def readParquet(
      spark: SparkSession,
      path: String,
      schema: Option[StructType] = None
  ): DataFrame

  /** Reads a DataFrame from JSON format.
    *
    * @param spark SparkSession for reading
    * @param path Path to JSON files
    * @param schema Optional schema to apply (default: None, infer from data)
    */
  def readJson(
      spark: SparkSession,
      path: String,
      schema: Option[StructType] = None
  ): DataFrame

  /** Reads a DataFrame from CSV format.
    *
    * @param spark SparkSession for reading
    * @param path Path to CSV files
    * @param schema Optional schema to apply (default: None, infer from data)
    * @param header Whether the file has a header row (default: true)
    * @param delimiter Field delimiter (default: ",")
    */
  def readCsv(
      spark: SparkSession,
      path: String,
      schema: Option[StructType] = None,
      header: Boolean = true,
      delimiter: String = ","
  ): DataFrame

  /** Reads a DataFrame from an Iceberg table.
    *
    * @param spark SparkSession for reading
    * @param tableName Fully qualified table name (e.g., "db.feature_table")
    */
  def readIcebergTable(
      spark: SparkSession,
      tableName: String
  ): DataFrame
}

/** Production implementation of Fetchers that reads from real storage. */
object ProdFetcher extends Fetchers {
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

/** Backward compatibility: Fetchers object points to ProdFetcher. */
object Fetchers extends Fetchers {
  def readParquet(
      spark: SparkSession,
      path: String,
      schema: Option[StructType] = None
  ): DataFrame = ProdFetcher.readParquet(spark, path, schema)

  def readJson(
      spark: SparkSession,
      path: String,
      schema: Option[StructType] = None
  ): DataFrame = ProdFetcher.readJson(spark, path, schema)

  def readCsv(
      spark: SparkSession,
      path: String,
      schema: Option[StructType] = None,
      header: Boolean = true,
      delimiter: String = ","
  ): DataFrame = ProdFetcher.readCsv(spark, path, schema, header, delimiter)

  def readIcebergTable(
      spark: SparkSession,
      tableName: String
  ): DataFrame = ProdFetcher.readIcebergTable(spark, tableName)
}
