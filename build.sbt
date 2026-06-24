ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.18"

val fs2KakfaVersion = "3.5.1"
val typesafeVersion = "1.4.3"
val circeVersion = "0.14.15"
val doobieVersion = "1.0.0-RC12"
val sqliteJdbcVersion = "3.49.0.0"
val log4CatsVersion = "2.8.0"
val logbackVersion =  "1.4.14"

lazy val root = (project in file("."))
  .settings(
    name := "personal-web-analytics",
    libraryDependencies ++= Seq(
      "com.github.fd4s" %% "fs2-kafka" % fs2KakfaVersion,
      "com.typesafe" % "config" % typesafeVersion,
      "io.circe" %% "circe-core" % circeVersion,
      "io.circe" %% "circe-generic" % circeVersion,
      "io.circe" %% "circe-parser" % circeVersion,
      "org.tpolecat" %% "doobie-core" % doobieVersion,
      "org.tpolecat" %% "doobie-hikari" % doobieVersion,
      "org.xerial" % "sqlite-jdbc" % sqliteJdbcVersion,
      "org.typelevel" %% "log4cats-slf4j" % log4CatsVersion,
      "ch.qos.logback" % "logback-classic" % logbackVersion
    )
  )