name := """spark-twitter-stream-example"""

version := "1.0.0"

scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.1",
  "org.apache.spark" %% "spark-sql" % "3.0.1",
  "org.apache.spark" %% "spark-streaming" % "3.0.1",
  "org.apache.bahir" %% "spark-streaming-twitter" % "2.4.0"
)