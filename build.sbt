name := "mimir-caveats"
version := "0.3.5"
organization := "org.mimirdb"
scalaVersion := "2.13.7"

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
resolvers += "MimirDB" at "https://maven.mimirdb.org/"
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
  "org.apache.spark"              %%  "spark-sql"                % "3.2.0" excludeAll(ExclusionRule(organization = "org.slf4j", name = "slf4j-log4j12"), ExclusionRule("org.apache.hadoop")),
  "org.apache.spark"              %%  "spark-mllib"              % "3.2.0" excludeAll(ExclusionRule(organization = "org.slf4j", name = "slf4j-log4j12"), ExclusionRule("org.apache.hadoop")),
  "org.apache.spark"              %%  "spark-hive"               % "3.2.0" excludeAll(ExclusionRule(organization = "org.slf4j", name = "slf4j-log4j12"), ExclusionRule("org.apache.hadoop")),
  "org.apache.hadoop"             %   "hadoop-client"            % "3.3.1" excludeAll(
                                                                              ExclusionRule(organization ="org.slf4j", name = "slf4j-log4j12"), 
                                                                              ExclusionRule("com.fasterxml.jackson.core"), 
                                                                            ),

  // Play JSON
  "com.typesafe.play"             %%  "play-json"                % "2.9.2"
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
