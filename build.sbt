name := "kafka-twitter"

version := "1.0"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka_2.11" % "0.11.0.2",
  "org.twitter4j" % "twitter4j-core" % "4.0.4",
  "org.twitter4j" % "twitter4j-stream" % "4.0.4",
  "com.typesafe" % "config" % "1.3.2",
  "com.twitter" % "bijection-core_2.11" % "0.9.5",
  "com.twitter" % "bijection-avro_2.11" % "0.9.5"
)