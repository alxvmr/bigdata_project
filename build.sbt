//val dottyVersion = "3.0.0-M2"
val dottyVersion = "2.12.10"

lazy val root = project
  .in(file("."))
  .settings(
      name := "dotty-simple",
      version := "0.1.0",

      scalaVersion := dottyVersion,

        libraryDependencies += "com.novocode" % "junit-interface" % "0.11" % "test",
        // https://mvnrepository.com/artifact/org.apache.spark/spark-core
        libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.1",
        // https://mvnrepository.com/artifact/org.apache.spark/spark-sql
        libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.1",
        // https://mvnrepository.com/artifact/org.apache.spark/spark-mllib
        //libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.1.1",
        libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.1.1",
        // https://mvnrepository.com/artifact/org.apache.bahir/spark-streaming-pubsub
        libraryDependencies += "org.apache.bahir" %% "spark-streaming-pubsub" % "2.4.0",
        libraryDependencies += "com.google.cloud.spark" %% "spark-bigquery-with-dependencies" % "0.16.1",
        libraryDependencies += "com.google.cloud" % "google-cloud-storage" % "1.70.0",
        // https://mvnrepository.com/artifact/com.google.cloud/google-cloud-storage
        //libraryDependencies += "com.google.cloud" % "pubsublite-spark-sql-streaming" % "0.1.0",
        libraryDependencies += "com.google.cloud" % "google-cloud-pubsub" % "1.56.0",
        // https://mvnrepository.com/artifact/com.google.cloud.spark/spark-bigquery
        libraryDependencies += "com.google.cloud.spark" % "spark-bigquery_2.12" % "0.16.1"
  )