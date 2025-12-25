import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Task2_2SQL {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Task2_2SQL")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    val inputPath = "file:///home/hadoop/projects/lab4/outputs/task1/online_consumption_table"

    val outputPath = "file:///home/hadoop/projects/lab4/outputs/task2/task2-2-merchant-pos-ratio"

    // 读取 Task1.2 输出结果, 每行格式：Merchant_id Neg Norm Pos（空格分隔）
    val rdd = spark.sparkContext
    .textFile(inputPath)
    .map(_.trim)
    .filter(_.nonEmpty)
    .map { line =>
        // 任意空白符分割
        val parts = line.split("\\s+")
        (parts(0), parts(1).toLong, parts(2).toLong, parts(3).toLong)
    }

    val merchantDF = rdd.toDF("Merchant_id", "Neg", "Norm", "Pos")

    // 计算正样本比例
    val resultDF = merchantDF
      .withColumn("Total", $"Neg" + $"Norm" + $"Pos")
      .filter($"Total" > 0)
      .withColumn(
        "Pos_Ratio",
        round($"Pos" / $"Total", 3)
      )
      .orderBy($"Pos_Ratio".desc)

    // 控制台输出 Top 10 
    println("===== Task 2.2 商家正样本比例 Top10 =====")
    resultDF
      .select("Merchant_id", "Pos_Ratio", "Pos", "Total")
      .show(10, truncate = false)

    // 保存完整结果
    resultDF
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "false")
      .option("sep", " ")
      .csv(outputPath)

    spark.stop()
  }
}
