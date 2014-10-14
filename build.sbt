
name := "Spark Cassandra Demo"

version := "0.1"

scalaVersion := "2.10.4"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.1.0" withSources() withJavadoc(),
  "org.apache.spark" %% "spark-sql" % "1.1.0" withSources() withJavadoc(),
  "org.apache.spark" %% "spark-streaming-twitter" % "1.1.0" withSources() withJavadoc(),
  "com.datastax.spark" %% "spark-cassandra-connector" % "1.1.0-alpha2" withSources() withJavadoc()
)


resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"