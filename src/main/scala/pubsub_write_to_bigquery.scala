import org.apache.spark
import org.apache.spark.sql.{Column, Row, SaveMode, SparkSession, functions}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.pubsub.{PubsubUtils, SparkGCPCredentials}
import org.apache.spark.sql.functions.{array, desc}

import java.nio.charset.StandardCharsets

object pubsub_write_to_bigquery {
  def main(args: Array[String]): Unit = {
    import org.apache.spark._

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("NetworkWordCount")

    val projectID = "myproject-310015"
    val subscription = "sport-sub"
    val topicId = "projects/myproject-310015/topics/sport"

    val bucket = "sgu251al_temporarygcsbucket"

    val ssc = new StreamingContext(conf, Seconds(10))

    val lines = PubsubUtils.createStream(
      ssc,
      projectID,
      Some("sport"),
      subscription, // Cloud Pub/Sub subscription for incoming tweets
      SparkGCPCredentials.builder.build(), StorageLevel.MEMORY_AND_DISK_SER_2
    )
      .map(message => new String(message.getData(), StandardCharsets.UTF_8))

    val schema = StructType(
      Seq(
        StructField(name = "NOC", dataType = StringType, nullable = false),
        StructField(name = "Year", dataType = StringType, nullable = false),
        StructField(name = "City", dataType = StringType, nullable = false),
        StructField(name = "Medal", dataType = StringType, nullable = false),
      )
    )

    val sc = SparkSession.builder()
      .master("local[*]")
      .appName("pub/sub streaming")
      .getOrCreate()

    import sc.implicits._

    sc.conf.set("temporaryGcsBucket", bucket)

    def limitSize(n: Int, arrCol: Column): Column =
      array( (0 until n).map( arrCol.getItem ): _* )

    lines.foreachRDD(rdd => {
      val rows = rdd.map(_.split(" ")).map(a => Row.fromSeq(a))
      val df = sc.createDataFrame(rows, schema)

      val res_1 = df
      val res_2 = df
      val res_3 = df
      res_1
        .filter(res_1("Medal") === "Gold" || res_1("Medal") === "Silver" || res_1("Medal") === "Bronze")
        .groupBy("Year")
        .count()
        //.show()
        .write.format("com.google.cloud.spark.bigquery")
        .option("table","myDataSet.pub_sub1")
        .mode(SaveMode.Overwrite)
        .save()

      res_2
        .filter(res_2("Medal") === "Gold" || res_2("Medal") === "Silver" || res_2("Medal") === "Bronze")
        .groupBy("City")
        .count()
        .sort($"count".desc)
        .limit(10)
        //.show()
        .write.format("com.google.cloud.spark.bigquery")
        .option("table","myDataSet.pub_sub2")
        .mode(SaveMode.Overwrite)
        .save()

      res_3
        .filter(res_3("Medal") === "Gold")
        .groupBy("Year", "NOC")
        .count()
        .sort($"count".desc)
        .groupBy("Year").agg(functions.collect_list("NOC").as("AllCountries"))
        .select($"Year", limitSize(10, $"AllCountries").as("Top10"))
        //.show()
        .write.format("com.google.cloud.spark.bigquery")
        .option("table","myDataSet.pub_sub3")
        .mode(SaveMode.Overwrite)
        .save()
    })

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }
}
