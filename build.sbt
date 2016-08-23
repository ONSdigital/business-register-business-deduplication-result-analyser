lazy val artifactSettings = Seq(
  organization := "uk.gov.ons.business-register",
  version := "1.0.0-SNAPSHOT",
  licenses := Seq("MIT" -> url("http://opensource.org/licenses/MIT"))
)

lazy val buildSettings = Seq(
  scalaVersion := "2.10.6",
  // Scala / Java options
  scalacOptions ++= Seq("-deprecation", "-unchecked"),
  javacOptions ++= Seq("-source", "1.7", "-target", "1.7"),
  javaOptions ++= Seq("-Xmx2G"),
  // Repositories
  resolvers ++= Seq(
    Resolver.defaultLocal,
    Resolver.mavenLocal
  )
)

lazy val consoleSettings = Seq(
  fork in console := true,
  initialCommands in console :=
    """ println("Welcome to\n" +
      |"      ____              __\n" +
      |"     / __/__  ___ _____/ /__\n" +
      |"    _\\ \\/ _ \\/ _ `/ __/  '_/\n" +
      |"   /___/ .__/\\_,_/_/ /_/\\_\\\n" +
      |"      /_/\n" +
      |"Using Scala \"%s\"\n")
      |
      |import org.apache.spark.SparkContext._
      |import org.apache.spark.sql.functions._
      |
      |val sc = {
      |  val conf = new org.apache.spark.SparkConf()
      |    .setMaster("%s")
      |    .setAppName("Console + Spark!")
      |  val _sc = new org.apache.spark.SparkContext(conf)
      |  println("Spark context available as sc.")
      |  _sc
      |}
      |
      |val sqlContext = {
      |  val _sqlContext = new org.apache.spark.sql.SQLContext(sc)
      |  println("SQL context available as sqlContext.")
      |  _sqlContext
      |}
      |import sqlContext.implicits._
      |import sqlContext.sql
    """.format(scalaVersion.value, sys.env.getOrElse("SPARK_MODE", "local[2]")).stripMargin,
  cleanupCommands in console :=
    s"""
       |sc.stop()
   """.stripMargin
)

lazy val assemblySettings = Seq(
  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", _*) => MergeStrategy.discard
    case _ => MergeStrategy.first
  }
)

// Modules

val sparkVersion = "1.6.2"
val sparkDependencyScope = "compile"

val root = Project("business-deduplication-result-analyser", file("."))
  .settings(artifactSettings: _*)
  .settings(buildSettings: _*)
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % sparkDependencyScope,
      "org.apache.spark" %% "spark-sql" % sparkVersion % sparkDependencyScope,
      "com.github.scopt" %% "scopt" % "3.5.0",
      "org.apache.commons" % "commons-csv" % "1.4",
      // test dependencies
      "uk.gov.ons.business-register" %% "test-utils" % "1.0.0-SNAPSHOT" % "test"
    ),
    fork := true
  )
  .settings(consoleSettings: _*)
  .settings(assemblySettings: _*)