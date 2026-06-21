ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.18"

lazy val root = (project in file("."))
  .settings(
    name := "personal-web-analytics",
    libraryDependencies ++= Seq(
      "com.github.fd4s" %% "fs2-kafka" % "3.5.1",
      "com.typesafe"    %  "config"    % "1.4.3",
    )
  )