package platform

object PlatformProvider {
  def create(
      appName: String,
      master: Option[String] = None,
      config: Map[String, String] = Map.empty
  ): SparkPlatformTrait = {
    new SparkPlatform(appName, master, config)
  }

  def createLocal(
      appName: String,
      config: Map[String, String] = Map.empty
  ): SparkPlatformTrait = {
    create(appName, Some("local[*]"), config)
  }
}

