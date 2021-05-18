import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.array
import org.apache.spark.sql.{Row, SaveMode, SparkSession, functions}
import com.google.cloud.spark.bigquery

object example_dataframe {
    def main(args: Array[String]): Unit = {
      Logger.getRootLogger.setLevel(Level.INFO)

      val ss = SparkSession
        .builder()
        .appName("Spark SQL basic example")
        .master("local[*]")
        .getOrCreate()

      import ss.implicits._
      var df = ss
        .read
        .format("csv")
        .option("header","true")
        .option("quote", "\"")
        .option("escape", "\"")
        .load(path = "gs://athlete_work/athlete_events.csv")

      val bucket = "sgu251al_temporarygcsbucket"
      ss.conf.set("temporaryGcsBucket", bucket)

      val res_1 = df
      res_1
        .filter(res_1("Medal") === "Gold" || res_1("Medal") === "Silver" || res_1("Medal") === "Bronze")
        .groupBy("Year")
        .count()
        .write.format("com.google.cloud.spark.bigquery")
        .option("table","myDataSet.result1")
        .mode(SaveMode.Overwrite)
        .save()

      val res_2 = df
      res_2
        .filter(res_2("Medal") === "Gold" || res_2("Medal") === "Silver" || res_2("Medal") === "Bronze")
        .groupBy("City")
        .count()
        .sort($"count".desc)
        .limit(10)
        .write.format("com.google.cloud.spark.bigquery")
        .option("table","myDataSet.result2")
        .mode(SaveMode.Overwrite)
        .save()
        //.show(10)

      def limitSize(n: Int, arrCol: Column): Column =
        array( (0 until n).map( arrCol.getItem ): _* )

      val res_3 = df
      val result = res_3
        .filter(res_3("Medal") === "Gold")
        .groupBy("Year", "NOC")
        .count()
        .sort($"count".desc)
        //.show(50)
        //.groupBy("Year").agg(functions.collect_list(array($"NOC", $"count")))
        .groupBy("Year").agg(functions.collect_list("NOC").as("AllCountries"))
        .select($"Year", limitSize(10, $"AllCountries").as("Top10"))

      result
        .write.format("com.google.cloud.spark.bigquery")
        .option("table","myDataSet.result3")
        .mode(SaveMode.Overwrite)
        .save()
    }
}
