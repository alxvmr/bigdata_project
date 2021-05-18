import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

object pubsub_task1 {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.INFO)

    //val conf = new SparkConf()
    //  .setMaster("local[*]")
    //  .setAppName("NetworkWordCount")
    //conf.set ("spark.sql.legacy.setCommandRejectsSparkCoreConfs", "false")
    //conf.set ("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible = true")
    //conf.set ("spark.executor. extraJavaOptions "," -Dio.netty.tryReflectionSetAccessible = true")

    val projectID = "myproject-310015"
    val tableID = "myproject-310015:myDataSet.pubsub"

    val sc = SparkSession
      .builder()
      .master("local[*]")
      .appName("pub/sub streaming")
      .getOrCreate()

    //sc.conf.set("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true")
    //sc.conf.set("spark.executor.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true")

    val bucket = "sgu251al_temporarygcsbucket"
    sc.conf.set("temporaryGcsBucket", bucket)

    val data = sc
      .read
      .format("bigquery")
      .option("table","myproject-310015:myDataSet.pubsub") //Failed to load any of the given libraries: [netty_tcnative_windows_x86_64, netty_tcnative_x86_64, netty_tcnative]
      .load()
      .cache()
    data.createOrReplaceTempView("words")

    val res_1 = data.toDF()
    res_1
      .filter(data("Medal") === "Gold" || data("Medal") === "Silver" || data("Medal") === "Bronze")
      .groupBy("Year")
      .count()

    res_1
      .write
      .format("bigquery")
      .option("table","myDataSet.pubsub_1")
      .option("temporaryGcsBucket", "sgu251al_temporarygcsbucket")
      .mode(SaveMode.Overwrite)
      .save()
  }
}
