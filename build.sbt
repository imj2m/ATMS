name := "ATMS_Proj"

version := "1.0"

scalaVersion := "2.10.4"

resolvers += "Job Server Bintray" at "https://dl.bintray.com/spark-jobserver/maven"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1"

libraryDependencies += "spark.jobserver" %% "job-server-api" % "0.6.1" % "provided"
