resolvers += "Sonatype OSS Releases" at "https://oss.sonatype.org/content/repositories/releases"

addSbtPlugin("com.tuplejump.com.github.hochgi" % "sbt-cassandra"   % "1.0.0")
addSbtPlugin("net.virtual-void"  % "sbt-dependency-graph"   % "0.8.2")
addSbtPlugin("de.heikoseeberger" % "sbt-header"             % "1.5.0")
addSbtPlugin("org.scalastyle"    %% "scalastyle-sbt-plugin" % "0.8.0")