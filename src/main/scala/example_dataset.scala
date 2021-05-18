import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.array
import org.apache.spark.sql.{Column, SaveMode, SparkSession, functions}
import com.google.cloud.bigquery.BigQuery

import java.io.Serializable
case class Event(NOC: String, year: String, City: String, Medal: String)

object example_dataset {
  def main(args: Array[String]): Unit ={
    Logger.getRootLogger.setLevel(Level.INFO)

    val ss = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local[*]")
      .getOrCreate()

    import ss.implicits._
    val bucket = "sgu251al_temporarygcsbucket"
    ss.conf.set("temporaryGcsBucket", bucket)

    val ds = ss
      .read
      .format("csv")
      .option("header","true")
      .option("quote", "\"")
      .option("escape", "\"")
      .load(path = "gs://athlete_work/athlete_events.csv")
      .as[Event]

    val res1_2 = ds
      //.filter(r => r.Medal != "NA")
      .filter(r => Set("Gold", "Silver", "Bronze").contains(r.Medal))
      .groupBy("Year")
      .count()

    res1_2
      .write.format("com.google.cloud.spark.bigquery")
      .option("table","myDataSet.result1_2")
      .mode(SaveMode.Overwrite)
      .save()

    val res2_2 = ds
      .filter(r => Set("Gold", "Silver", "Bronze").contains(r.Medal))
      .groupBy("City")
      .count()
      .sort($"count".desc)
      .limit(10)

    res2_2
      .write.format("com.google.cloud.spark.bigquery")
      .option("table","myDataSet.result2_2")
      .mode(SaveMode.Overwrite)
      .save()

    def limitSize(n: Int, arrCol: Column): Column =
      array( (0 until n).map( arrCol.getItem ): _* )

    val res3_2 = ds
      .filter(r=>r.Medal == "Gold")
      .groupBy("Year", "NOC")
      .count()
      .sort($"count".desc)
      .groupBy("Year").agg(functions.collect_list("NOC").as("AllCountries"))
      .select($"Year", limitSize(10, $"AllCountries").as("Top10"))

    res3_2
      .write.format("com.google.cloud.spark.bigquery")
      .option("table","myDataSet.result3_2")
      .mode(SaveMode.Overwrite)
      .save()
  }
}
