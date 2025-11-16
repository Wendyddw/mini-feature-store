package platform

import org.apache.spark.sql.DataFrame

object Writers {
  def writeParquet(
      df: DataFrame,
      path: String,
      mode: String = "overwrite",
      partitionBy: Seq[String] = Seq.empty
  ): Unit = {
    val writer = df.write.mode(mode)
    if (partitionBy.nonEmpty) {
      writer.partitionBy(partitionBy: _*).parquet(path)
    } else {
      writer.parquet(path)
    }
  }

  def writeJson(
      df: DataFrame,
      path: String,
      mode: String = "overwrite"
  ): Unit = {
    df.write.mode(mode).json(path)
  }

  def writeCsv(
      df: DataFrame,
      path: String,
      mode: String = "overwrite",
      header: Boolean = true,
      delimiter: String = ","
  ): Unit = {
    df.write
      .mode(mode)
      .option("header", header)
      .option("delimiter", delimiter)
      .csv(path)
  }
}

