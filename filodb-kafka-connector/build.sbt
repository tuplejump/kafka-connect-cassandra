name := "filodb-kafka-connector"

version := "0.0.1"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "connect-api" % "0.9.0.0",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test"
)