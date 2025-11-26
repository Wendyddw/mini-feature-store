ThisBuild / scalaVersion := "2.13.12"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "com.example"

val sparkVersion = "3.5.0"
val icebergVersion = "1.4.2"
val redisVersion = "4.0.1"

lazy val root = (project in file("."))
  .settings(
    name := "mini-feature-store-spark",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
      "org.apache.iceberg" %% "iceberg-spark-runtime-3.5" % icebergVersion,
      "redis.clients" % "jedis" % redisVersion,
      "org.scalatest" %% "scalatest" % "3.2.18" % Test,
      "org.apache.spark" %% "spark-core" % sparkVersion % Test classifier "tests",
      "org.apache.spark" %% "spark-sql" % sparkVersion % Test classifier "tests"
    ),
    assembly / mainClass := Some("com.example.featurestore.App"),
    assembly / assemblyJarName := "mini-feature-store-spark.jar",
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x => MergeStrategy.first
    },
    Test / fork := true,
    // Use Java 17 for tests (Spark 3.5.0 compatibility)
    Test / javaHome := {
      val java17Home = file("/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home")
      if (java17Home.exists()) Some(java17Home) else None
    },
    Test / javaOptions ++= Seq(
      "-Xmx2G",
      "-XX:+UseG1GC",
      "-Dlog4j2.configurationFile=log4j2-test.xml",
      "-Dhadoop.home.dir=/tmp",
      // Open modules for Spark compatibility with Java 17
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
      "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
    ),
    Compile / javaOptions ++= Seq(
      "-Xmx2G",
      "-XX:+UseG1GC"
    ),
    scalacOptions ++= Seq(
      "-deprecation",
      "-feature",
      "-unchecked",
      "-Xlint",
      "-Ywarn-dead-code",
      "-Ywarn-unused"
    )
  )

