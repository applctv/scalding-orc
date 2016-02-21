name := "scalding.orc"

organization := "io.applicative"

scalaVersion := "2.11.7"

version := "0.1.0-SNAPSHOT"

licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0.html"))

bintrayOrganization := Some("applctv")

bintrayRepository := "scalding-orc"

bintrayVcsUrl := Some("git@github.com:applctv/scalding-orc.git")

// Hadoop mini-cluster locking prevents parallel execution
parallelExecution in Test := false

// Publish settings for Maven Central
publishMavenStyle := true
pomExtra := (
    <url>https://github.com/applctv/scalding-orc/</url>
    <scm>
      <url>git@github.com:applctv/scalding-orc.git</url>
      <connection>scm:git:git@github.com:applctv/scalding-orc.git</connection>
    </scm>
    <developers>
      <developer>
        <id>applctv</id>
        <name>Applicative</name>
        <url>http://applicative.io</url>
      </developer>
    </developers>)


resolvers ++= Seq(
  "Concurrent Maven Repo" at "http://conjars.org/repo",
  "Clojars Repo" at "http://clojars.org/repo",
  "Twitter Maven" at "http://maven.twttr.com",
  "Local Maven" at Path.userHome.asFile.toURI.toURL + ".m2/repository"
)

libraryDependencies ++= Seq(
  "com.twitter" %% "scalding-core" % "0.15.0",
  "com.twitter" %% "scalding-macros" % "0.15.0",
  "com.twitter" %% "bijection-core" % "0.8.0",
  "org.apache.hadoop" % "hadoop-core" % "1.2.1" % "provided",
  "org.apache.hive" % "hive-exec" % "1.0.0",
  "org.apache.hive" % "hive-serde" % "1.0.0",
  "com.hotels" % "corc-cascading" % "1.1.0",
  "com.hotels" % "corc-core" % "1.1.0",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "com.twitter" %% "scalding-hadoop-test" % "0.15.0" % "test"
)
