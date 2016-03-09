name := "kafka-connect-cassandra"

version := "0.0.4"

crossScalaVersions := Seq("2.11.7", "2.10.6")

crossVersion := CrossVersion.binary

scalaVersion := sys.props.getOrElse("scala.version", crossScalaVersions.value.head)

organization := "com.tuplejump"

description := "A Kafka Connect Cassandra Source and Sink connector."

licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

import de.heikoseeberger.sbtheader.license.Apache2_0

de.heikoseeberger.sbtheader.HeaderPlugin.autoImport.headers := Map(
  "scala" -> Apache2_0("2016", "Tuplejump"),
  "conf"  -> Apache2_0("2016", "Tuplejump", "#")
)

lazy val cassandra = sys.props.getOrElse("cassandra.version","3.0.0")//latest: 3.0.4

libraryDependencies ++= Seq(
  "org.apache.kafka"       % "connect-api"           % "0.9.0.1"    % "provided",
  "com.datastax.cassandra" % "cassandra-driver-core" % cassandra,   //was: 2.1.9
  "org.scalatest"          %% "scalatest"            % "2.2.6"       % "test,it",
  "org.mockito"            % "mockito-core"          % "2.0.34-beta" % "test,it"
)

import com.github.hochgi.sbt.cassandra._

CassandraPlugin.cassandraSettings

test in IntegrationTest <<= stopCassandra.dependsOn(test in IntegrationTest).dependsOn(startCassandra)
testOnly in IntegrationTest <<= (testOnly in IntegrationTest).dependsOn(startCassandra)

cassandraVersion := "2.2.2"

cassandraCqlInit := "src/it/resources/setup.cql"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

lazy val root = (project in file(".")).enablePlugins(BuildInfoPlugin).settings(
  buildInfoKeys := Seq[BuildInfoKey](version),
  buildInfoPackage := "com.tuplejump.kafka.connector",
  buildInfoObject := "CassandraConnectorInfo"
)

