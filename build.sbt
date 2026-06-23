ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.18"

val circeVersion = "0.14.15"
val typesafeVersion = "1.4.3"
val fs2KakfaVersion = "3.5.1"

lazy val root = (project in file("."))
  .settings(
    name := "personal-web-analytics",
    libraryDependencies ++= Seq(
      "com.github.fd4s" %% "fs2-kafka" % fs2KakfaVersion,
      "com.typesafe" % "config" % typesafeVersion,
      "io.circe" %% "circe-core" % circeVersion,
      "io.circe" %% "circe-generic" % circeVersion,
      "io.circe" %% "circe-parser" % circeVersion,
      "org.tpolecat" %% "doobie-core" % "1.0.0-RC12",
      "org.tpolecat" %% "doobie-hikari" % "1.0.0-RC12",
      "org.xerial" % "sqlite-jdbc" % "3.49.0.0",
    )
  )