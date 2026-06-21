ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.18"

val circeVersion = "0.14.15"

lazy val root = (project in file("."))
  .settings(
    name := "personal-web-analytics",
    libraryDependencies ++= Seq(
      "com.github.fd4s" %% "fs2-kafka" % "3.5.1",
      "com.typesafe" % "config" % "1.4.3",
      "io.circe" %% "circe-core" % circeVersion,
      "io.circe" %% "circe-generic" % circeVersion,
      "io.circe" %% "circe-parser" % circeVersion
    )
  )