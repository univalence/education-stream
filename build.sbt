ThisBuild / scalaVersion := "2.13.10"

val libVersion = new {
  val kafka = "3.3.1"
  val gson = "2.10"
  val slf4j = "1.7.36"
  val logback = "1.2.11"
  val testcontainers = "1.17.6"
  val confluentPlatform = "7.1.1"

}

lazy val root =
  (project in file("."))
    .settings(
      name := "education-stream",
      libraryDependencies ++= Seq(
        "org.apache.kafka" % "kafka-clients" % libVersion.kafka,
        "org.apache.kafka" % "kafka-streams" % libVersion.kafka,
        "com.google.code.gson" % "gson" % libVersion.gson,
        "org.testcontainers" % "kafka" % libVersion.testcontainers,
        "org.slf4j" % "slf4j-api" % libVersion.slf4j,
        "ch.qos.logback" % "logback-classic" % libVersion.logback,
        "io.confluent" % "kafka-avro-serializer" % libVersion.confluentPlatform
      )
    )

Global / onChangedBuildSource := ReloadOnSourceChanges

resolvers += "confluent" at "https://packages.confluent.io/maven/"
