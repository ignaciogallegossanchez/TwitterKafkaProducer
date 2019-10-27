name := "twitterproducer"
version := "1.0"
scalaVersion := "2.11.12"

libraryDependencies += "com.twitter" % "hbc-core" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.0"