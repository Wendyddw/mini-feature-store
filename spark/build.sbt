ThisBuild / scalaVersion := "2.13.12"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "com.example"

// Enable semanticdb for scalafix
ThisBuild / semanticdbEnabled := true
ThisBuild / semanticdbVersion := "4.8.11"

val sparkVersion = "3.5.0"
val icebergVersion = "1.4.2"
val redisVersion = "4.0.1"
val hadoopVersion = "3.3.6" // Hadoop version compatible with Spark 3.5.0

lazy val root = (project in file("."))
  .settings(
    name := "mini-feature-store-spark",
    libraryDependencies ++= Seq(
      // Spark dependencies - not marked as "provided" to allow local runs via sbt runMain
      // Assembly will still create a fat JAR with all dependencies
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.iceberg" %% "iceberg-spark-runtime-3.5" % icebergVersion,
      // Hadoop AWS S3A for MinIO/S3 access
      "org.apache.hadoop" % "hadoop-aws" % hadoopVersion,
      "org.apache.hadoop" % "hadoop-common" % hadoopVersion,
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
    // Fork JVM for local runs to ensure proper classpath and isolation
    Compile / run / fork := true,
    // Use Java 17 for both tests and local runs (Spark 3.5.0 compatibility, avoids Java 25+ Subject API issues)
    Test / javaHome := {
      val java17Home = file("/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home")
      if (java17Home.exists()) Some(java17Home) else None
    },
    Compile / run / javaHome := {
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
    Compile / run / javaOptions ++= Seq(
      "-Xmx2G",
      "-XX:+UseG1GC",
      "-Dhadoop.home.dir=/tmp",
      // Workaround for Java 17+ Subject API removal
      "-Dhadoop.security.authentication=simple",
      "-Djava.security.auth.login.config=",
      // Open modules for Spark compatibility with Java 17+
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
      "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED",
      "--add-opens=java.base/javax.security.auth=ALL-UNNAMED",
      "--add-opens=java.base/java.security=ALL-UNNAMED",
      // Enable native access for Hadoop
      "--enable-native-access=ALL-UNNAMED"
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

