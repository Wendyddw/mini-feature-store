ThisBuild / scalaVersion := "2.13.12"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "com.example"

val sparkVersion = "3.5.0"

lazy val root = (project in file("."))
  .settings(
    name := "mini-feature-store-spark",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
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
    Test / javaOptions ++= Seq(
      "-Xmx2G",
      "-XX:+UseG1GC",
      "-Dlog4j2.configurationFile=log4j2-test.xml"
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

