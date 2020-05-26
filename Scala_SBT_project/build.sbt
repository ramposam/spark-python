name := "Scala_SBT_project"
version := "0.1"
scalaVersion := "2.11.12"
libraryDependencies += "com.typesafe" % "config"%"1.3.2"
/*libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.36"*/
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.4"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4"
/*libraryDependencies += "org.apache.spark" % "spark-hive2.10" % "1.6.2"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.6.2"
libraryDependencies += "org.apache.spark" % "spark-streaming-flume_2.10" % "1.6.2"
libraryDependencies += "org.apache.spark" % "spark-streaming-flume-sink_2.10" % "1.6.2"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.2"
*/

  /*sparkVersion = "2.3.0"
  resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
  )
  libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.apache.spark" % "spark-streaming-flume_2.11" % "2.3.2",
  "org.apache.spark" % "spark-streaming-flume-sink_2.11" % "2.3.2",
  "org.apache.commons" % "commons-lang3" % "3.2.1",
  "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.2",
  "org.scala-lang" % "scala-library" % "2.11.7",
  "mysql" % "mysql-connector-java" % "5.1.6"
  )*/