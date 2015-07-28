name := "scalding.orc"

organization := "io.applicative"

scalaVersion := "2.11.6"

version := "0.0.1"

resolvers ++= Seq(
  "Concurrent Maven Repo" at "http://conjars.org/repo",
  "Clojars Repo" at "http://clojars.org/repo",
  "Twitter Maven" at "http://maven.twttr.com"
)

libraryDependencies ++= Seq(
  "com.twitter" %% "scalding-core" % "0.15.0",
  "com.twitter" %% "scalding-macros" % "0.15.0",
  "org.apache.hadoop" % "hadoop-core" % "1.2.1" % "provided",
  "org.apache.hive" % "hive-exec" % "1.0.0" % "provided",
  "org.apache.hive" % "hive-serde" % "1.0.0" % "provided",
  "com.hotels" % "corc-cascading" % "1.0.0",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "com.twitter" %% "scalding-hadoop-test" % "0.15.0" % "test"
)
