ThisBuild / version := "1.0.0"

ThisBuild / scalaVersion := "3.1.3"

val logbackVersion = "1.3.0"
val sfl4sVersion = "2.0.3"
val typesafeConfigVersion = "1.4.2"
val scalacticVersion = "3.2.9"
val hadoopVersion = "3.3.4"

lazy val root = (project in file("."))
  .settings(
    name := "CS-441-Project-1"
  )

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion,
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % hadoopVersion,
  "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % hadoopVersion,
  "ch.qos.logback" % "logback-core" % logbackVersion,
  "ch.qos.logback" % "logback-classic" % logbackVersion,
  "org.slf4j" % "slf4j-api" % sfl4sVersion,
  "org.slf4j" % "slf4j-simple" % sfl4sVersion,
  "com.typesafe" % "config" % typesafeConfigVersion,
  "org.scala-lang" % "scala-reflect" % "2.13.10",
  "org.scalactic" %% "scalactic" % scalacticVersion,
  "org.scalatest" %% "scalatest" % scalacticVersion % Test,
  "org.scalatest" %% "scalatest-featurespec" % scalacticVersion % Test
)


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
