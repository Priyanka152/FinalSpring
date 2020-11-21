name := "FinalSpring"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.7"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.7"
libraryDependencies += "org.apache.spark" %% "spark-streaming"  % "2.4.7"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.7"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.3.1"


