
name := "Spark Cassandra Demo"

version := "0.1"

scalaVersion := "2.10.4"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.2.0" withSources() withJavadoc(),
  "org.apache.spark" %% "spark-sql" % "1.2.0" withSources() withJavadoc(),
  "org.apache.spark" %% "spark-streaming" % "1.2.0" withSources() withJavadoc(),
  "org.apache.spark" %% "spark-streaming-twitter" % "1.2.0" withSources() withJavadoc(),
  "com.datastax.spark" %% "spark-cassandra-connector" % "1.2.0-alpha1" withSources() withJavadoc(),
  "com.github.nscala-time" %% "nscala-time" % "1.6.0" withSources() withJavadoc()
)


resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"