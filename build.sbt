name := "NIL1"

version := "0.1"

scalaVersion := "2.12.7"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.0",
  "org.apache.spark" %% "spark-sql" % "2.4.0",
  "org.apache.spark" %% "spark-streaming" % "2.4.0",
  "org.apache.spark" %% "spark-avro" % "2.4.0",
  "org.apache.kafka" % "kafka-clients" % "0.10.0.0",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.0" % "provided",
  "org.json4s" %% "json4s-jackson" % "3.7.0-M6",
  "org.scala-lang.modules" %% "scala-xml" % "2.0.0-M1",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"

)