name := "mimir-caveats"
version := "0.2.4"
organization := "org.mimirdb"
scalaVersion := "2.12.10"

// Make the UX work in SBT
fork := true
outputStrategy in run := Some(StdoutOutput)
connectInput in run := true
cancelable in Global := true

// Produce Machine-Readable JUnit XML files for tests
testOptions in Test ++= Seq( Tests.Argument("junitxml"), Tests.Argument("console") )

// include test classes
Test / publishArtifact := true

// Specs2 Requirement:
scalacOptions in Test ++= Seq("-Yrangepos")

// Support Test Resolvers
resolvers += "MimirDB" at "https://maven.mimirdb.info/"
resolvers += Resolver.typesafeRepo("releases")
resolvers += DefaultMavenRepository
resolvers ++= Seq("snapshots", "releases").map(Resolver.sonatypeRepo)

// Custom Dependencies
libraryDependencies ++= Seq(
  // Logging
  "com.typesafe.scala-logging"    %%  "scala-logging"            % "3.9.2",
  "ch.qos.logback"                %   "logback-classic"          % "1.2.3",
  "org.jline"                     %   "jline"                    % "3.16.0",

  // Testing
  "org.specs2"                    %%  "specs2-core"              % "4.8.2" % "test",
  "org.specs2"                    %%  "specs2-matcher-extra"     % "4.8.2" % "test",
  "org.specs2"                    %%  "specs2-junit"             % "4.8.2" % "test",

  // Spark
  "org.apache.spark"              %%  "spark-sql"                % "3.0.0" excludeAll(ExclusionRule(organization = "org.slf4j", name = "slf4j-log4j12"), ExclusionRule("org.apache.hadoop"), ExclusionRule("com.fasterxml.jackson.core")),
  "org.apache.spark"              %%  "spark-mllib"              % "3.0.0" excludeAll(ExclusionRule(organization = "org.slf4j", name = "slf4j-log4j12"), ExclusionRule("org.apache.hadoop"), ExclusionRule("com.fasterxml.jackson.core")),
  "org.apache.spark"              %%  "spark-hive"               % "3.0.0" excludeAll(ExclusionRule(organization = "org.slf4j", name = "slf4j-log4j12"), ExclusionRule("org.apache.hadoop"), ExclusionRule("com.fasterxml.jackson.core")),
  "org.apache.hadoop"             %   "hadoop-client"            % "2.8.2" excludeAll(ExclusionRule(organization ="org.slf4j", name = "slf4j-log4j12"), ExclusionRule("com.fasterxml.jackson.core")),

  // Play JSON
  "com.typesafe.play"             %%  "play-json"                % "2.8.1"
)

////// Publishing Metadata //////
// use `sbt publish make-pom` to generate
// a publishable jar artifact and its POM metadata

publishMavenStyle := true

pomExtra := <url>http://mimirdb.info</url>
  <licenses>
    <license>
      <name>Apache License 2.0</name>
      <url>http://www.apache.org/licenses/</url>
      <distribution>repo</distribution>
    </license>
  </licenses>
  <scm>
    <url>git@github.com:ubodin/mimir-caveats.git</url>
    <connection>scm:git:git@github.com:ubodin/mimir-caveats.git</connection>
  </scm>

/////// Publishing Options ////////
// use `sbt publish` to update the package in
// your own local ivy cache
publishTo := Some(Resolver.file("file",  new File("/var/www/maven_repo/")))
// publishTo := Some(Resolver.file("file",  new File(Path.userHome.absolutePath+"/.m2/repository")))
