name := "cassandra-kafka-connector"

version := "0.0.1"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq("org.apache.kafka" % "connect-api" % "0.9.0.0",
  "com.datastax.cassandra" % "cassandra-driver-core" % "2.1.9",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "org.mockito" % "mockito-core" % "2.0.34-beta" % "test"
)

import com.github.hochgi.sbt.cassandra._

CassandraPlugin.cassandraSettings

test in Test <<= (test in Test).dependsOn(startCassandra)

cassandraVersion := "2.2.2"

cassandraCqlInit := "src/test/resources/setup.cql"

lazy val root = (project in file(".")).enablePlugins(BuildInfoPlugin).settings(
  buildInfoKeys := Seq[BuildInfoKey](version),
  buildInfoPackage := "com.tuplejump.kafka.connector.cassandra"
)

