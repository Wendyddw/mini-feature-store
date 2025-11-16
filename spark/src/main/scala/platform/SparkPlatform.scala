package platform

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class SparkPlatform(
    appName: String,
    master: Option[String] = None,
    config: Map[String, String] = Map.empty
) extends SparkPlatformTrait {

  private val sparkConf = new SparkConf()
    .setAppName(appName)
    .setAll(config)

  master.foreach(sparkConf.setMaster)

  val spark: SparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

  def stop(): Unit = {
    spark.stop()
  }
}

