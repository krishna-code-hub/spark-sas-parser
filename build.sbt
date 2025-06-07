organization := "com.yourorg.spark"

name := "spark-sas-datasource"
version := "1.0"
scalaVersion := "2.12.15"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.4.0",
  "com.epam" % "parso" % "2.0.14",
  "org.apache.hadoop" % "hadoop-common" % "3.3.4",         // Required for FileSystem
  "org.apache.hadoop" % "hadoop-hdfs" % "3.3.4"             // Required for HDFS
)

