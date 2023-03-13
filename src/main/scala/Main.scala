import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SimpleApp {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder()
      .appName("Simple Application")
      .master("local[*]")
      .getOrCreate()

    val dfCSV = spark.read.format("csv")
      .option("pathGlobFilter", "*.csv")
      .option("header", true)
      .load("src/main/scala/resources")
      .toDF("first", "second")

    val dfTSV = spark.read.format("csv")
      .option("sep","\t")
      .option("pathGlobFilter", "*.tsv")
      .option("header", true)
      .load("src/main/scala/resources")
      .toDF("first", "second")
    val df = dfCSV.union(dfTSV)

    // BITWISE XOR tricky 
    // val count_odd_occurrence = udf((s: Seq[Int]) => s.reduce(_^_))
    val count_odd_occurrence = udf((s: Seq[Int]) =>
      s.groupBy(identity) // Map(1 -> List(1,1), 2 -> List(2,2,2))
        .filter { case (_, seq) => seq.length % 2 != 0 } // Map(2, List(2,2,2))
        .values // List(List(2,2,2))
        .head // List(2,2,2)
        .head // 2
    )

    val resultDf = df
      .groupBy("first")
      .agg(collect_list(col("second").cast("int")).as("second"))
      .withColumn("value", count_odd_occurrence(col("second")))
      .drop("second")
      .withColumnRenamed("first", "key")
    
    resultDf.write.option("delimiter", "\t").option("encoding", "UTF-8").csv("src/main/scala/resources/out.tsv")

    spark.stop()
  }
}
