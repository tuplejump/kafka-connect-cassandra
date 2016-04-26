
name := "kafka-connect-cassandra"

version := "0.0.7"

crossScalaVersions := Seq("2.11.7", "2.10.6")

crossVersion := CrossVersion.binary

scalaVersion := sys.props.getOrElse("scala.version", crossScalaVersions.value.head)

organization := "com.tuplejump"

organizationHomepage := Some(new java.net.URL("http://www.tuplejump.com"))

description := "A Kafka Connect Cassandra Source and Sink connector."

licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

import de.heikoseeberger.sbtheader.license.Apache2_0

de.heikoseeberger.sbtheader.HeaderPlugin.autoImport.headers := Map(
  "scala" -> Apache2_0("2016", "Tuplejump"),
  "conf"  -> Apache2_0("2016", "Tuplejump", "#")
)

lazy val cassandra = sys.props.getOrElse("cassandra.version","3.0.0")//latest: 3.0.4

libraryDependencies ++= Seq(
  "org.apache.kafka"       % "connect-api"            % "0.9.0.1"     % "provided",
  "com.datastax.cassandra" % "cassandra-driver-core"  % cassandra,
  //"com.typesafe.akka"      %% "akka-stream"          % "2.4.2"       % "provided",
  "joda-time"              %  "joda-time"             % "2.9.3",
  "org.joda"               %  "joda-convert"          % "1.8.1",
  "org.scalatest"          %% "scalatest"             % "2.2.6"       % "test,it",
  "org.mockito"            % "mockito-core"           % "2.0.34-beta" % "test,it",
  "ch.qos.logback"         % "logback-classic"        % "1.1.3"       % "test,it",
  CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, minor)) if minor < 11 =>
      "org.slf4j"                  % "slf4j-api"      % "1.7.13"
    case _ =>
      "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0"
  }
)

publishMavenStyle := true

/* Compiler settings and checks, code checks and compliance: */
cancelable in Global := true

lazy val sourceEncoding = "UTF-8"

scalacOptions ++= Seq(
  "-Xfatal-warnings",
  "-deprecation",
  "-feature",
  "-language:_",
  "-unchecked",
  "-Xlint",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-encoding", sourceEncoding
)

scalacOptions ++= (
  CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, minor)) if minor < 11 => Seq.empty
    case _ => Seq("-Ywarn-unused-import")
  })

javacOptions ++= Seq(
  "-Xmx1G",
  "-Xlint:unchecked",
  "-Xlint:deprecation",
  "-encoding", sourceEncoding
)

evictionWarningOptions in update := sbt.EvictionWarningOptions.default
  .withWarnTransitiveEvictions(false)
  .withWarnDirectEvictions(false)
  .withWarnScalaVersionEviction(false)

lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")

lazy val testScalastyle = taskKey[Unit]("testScalastyle")

import org.scalastyle.sbt.ScalastylePlugin

ScalastylePlugin.scalastyleFailOnError := true

testScalastyle := ScalastylePlugin.scalastyle.in(Test).toTask("").value

compileScalastyle := ScalastylePlugin.scalastyle.in(Compile).toTask("").value

/* Test, IntegrationTest */
lazy val testOptionsSettings = Tests.Argument(TestFrameworks.ScalaTest, "-oDF")

lazy val testConfigSettings = inConfig(Test)(Defaults.testTasks) ++
  inConfig(IntegrationTest)(Defaults.itSettings)

lazy val testSettings = testConfigSettings ++ Seq(
  fork in IntegrationTest := false,
  fork in Test := true,
  parallelExecution in IntegrationTest := false,
  parallelExecution in Test := true,
  testOptions in Test += testOptionsSettings,
  testOptions in IntegrationTest += testOptionsSettings,
  (internalDependencyClasspath in IntegrationTest) <<= Classpaths.concat(
    internalDependencyClasspath in IntegrationTest, exportedProducts in Test
  ))

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

pomExtra :=
  <scm>
    <url>git@github.com:tuplejump/kafka-connect-cassandra.git</url>
    <connection>scm:git:git@github.com:tuplejump/kafka-connect-cassandra.git</connection>
  </scm>
    <developers>
      <developer>
        <id>Shiti</id>
        <name>Shiti Saxena</name>
        <url>https://twitter.com/eraoferrors</url>
      </developer>
      <developer>
        <id>helena</id>
        <name>Helena Edelson</name>
        <url>https://twitter.com/helenaedelson</url>
      </developer>
    </developers>

publishTo <<= version {
  (v: String) =>
    val nexus = "https://oss.sonatype.org/"
    if (v.trim.endsWith("SNAPSHOT"))
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases" at nexus + "service/local/staging/deploy/maven2")
}

publishArtifact in Test := false

pomIncludeRepository := {
  _ => false
}

pomIncludeRepository := { _ => false }

lazy val root = (project in file("."))
  .enablePlugins(BuildInfoPlugin)
  .enablePlugins(AutomateHeaderPlugin)
  .enablePlugins(CassandraITPlugin)
  .settings(
    buildInfoKeys := Seq[BuildInfoKey](version),
    buildInfoPackage := "com.tuplejump.kafka.connect.cassandra",
    buildInfoObject := "CassandraConnectorInfo",
    cassandraVersion := cassandra,
    cassandraCqlInit := "src/it/resources/setup.cql",
    cassandraStartDeadline := 40
  )
  .settings(testSettings)
  .configs(IntegrationTest)
