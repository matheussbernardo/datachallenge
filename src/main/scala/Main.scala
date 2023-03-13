import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SimpleApp {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder()
      .appName("Simple Application")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read
      .option("header", true)
      .csv("src/main/scala/resources/sheet1.csv")
      .toDF("first", "second")


    // BITWISE XOR tricky 
    // val count_odd_occurrence = udf((s: Seq[Int]) => s.reduce(_^_))
    val count_odd_occurrence = udf((s: Seq[Int]) =>
      s.groupBy(identity) // Map(1 -> List(1,1), 2 -> List(2,2,2))
        .filter { case (_, seq) => seq.length % 2 != 0 } // Map(2, List(2,2,2))
        .values // List(List(2,2,2))
        .head // List(2,2,2)
        .head // 2
    )

    df
      .groupBy("first")
      .agg(collect_list(col("second").cast("int")).as("second"))
      .withColumn("value", count_odd_occurrence(col("second")))
      .withColumnRenamed("first", "key")
      .show()

    spark.stop()
  }
}
