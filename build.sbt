// Rename this as you see fit
name := "simpletiler"

version := "0.0.1"

scalaVersion := "2.11.12"

organization := "com.ken"

licenses := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html"))

scalacOptions ++= Seq(
  "-deprecation",
  "-unchecked",
  "-Yinline-warnings",
  "-language:implicitConversions",
  "-language:reflectiveCalls",
  "-language:higherKinds",
  "-language:postfixOps",
  "-language:existentials")

publishMavenStyle := true
publishArtifact in Test := false
pomIncludeRepository := { _ => false }

resolvers ++= Seq(
//  DefaultMavenRepository,
//  "MavenRepository" at "http://central.maven.org/maven2",
//  "Akka Repository" at "http://repo.akka.io/releases/",
//  "JBoss 3PP" at "https://repository.jboss.org/nexus/content/repositories/thirdparty-releases/",
  "locationtech-releases" at "https://repo.locationtech.org/content/groups/releases",
  "locationtech-snapshots" at "https://repo.locationtech.org/content/groups/snapshots",
  "Boundless Repository" at "http://repo.boundlessgeo.com/main/",
  "OSGeo Repository" at "http://download.osgeo.org/webdav/geotools/"
)

libraryDependencies ++= Seq(
//  "org.locationtech.geotrellis" %% "geotrellis-spark" % "1.2.0-RC2",
  "org.locationtech.geotrellis" %% "geotrellis-spark" % "1.2.1",
  "org.locationtech.geotrellis" %% "geotrellis-proj4" % "1.2.1",
  "org.locationtech.geotrellis" %% "geotrellis-geotools" % "1.2.1",

  "org.apache.spark"      %% "spark-core"       % "2.2.0" % Provided,
  "org.scalatest"         %%  "scalatest"       % "2.2.0" % Test,
  "org.apache.hadoop" % "hadoop-client"         % "2.7.5",
  "com.lihaoyi" %% "pprint" % "0.4.3"

//  "ch.cern.sparkmeasure" %% "spark-measure" % "0.11"
)

// When creating fat jar, remote some files with
// bad signatures and resolve conflicts by taking the first
// versions of shared packaged types.
assemblyMergeStrategy in assembly := {
  case "reference.conf" => MergeStrategy.concat
  case "application.conf" => MergeStrategy.concat
  case "META-INF/MANIFEST.MF" => MergeStrategy.discard
  case "META-INF\\MANIFEST.MF" => MergeStrategy.discard
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.discard
  case "META-INF/ECLIPSEF.SF" => MergeStrategy.discard
  case _ => MergeStrategy.first
}

initialCommands in console := """
 |import geotrellis.raster._
 |import geotrellis.vector._
 |import geotrellis.proj4._
 |import geotrellis.spark._
 |import geotrellis.spark.io._
 |import geotrellis.spark.io.hadoop._
 |import geotrellis.spark.tiling._
 |import geotrellis.spark.util._
 """.stripMargin
