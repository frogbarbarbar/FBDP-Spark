import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Task 2.1
 * 统计优惠券使用时间分布（上旬 / 中旬 / 下旬）
 * 使用 DateType + dayofmonth 实现
 */
object Task2_1SQL {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Task2_1SQL_DateType")
      .master("local[*]")
      .config("spark.sql.session.timeZone", "GMT+8")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    val inputPath = "file:///home/hadoop/projects/lab4/data/ccf_offline_stage1_train.csv"
    val outputPath = "file:///home/hadoop/projects/lab4/outputs/task2/task2-1-coupon-time-distribution"

    //读取数据
    val offlineDF = spark.read
      .option("header", "true")
      .option("nullValue", "null")
      .csv(inputPath)

    // 只保留“使用了优惠券”的记录，条件：Coupon_id != null AND Date != null
    val usedDF = offlineDF
      .filter($"Coupon_id".isNotNull && $"Date".isNotNull)
      // yyyyMMdd → DateType
      .withColumn("date_parsed", to_date($"Date", "yyyyMMdd"))
      .filter($"date_parsed".isNotNull)

    // 提取日，并划分时间段
    val periodDF = usedDF
      .withColumn("day", dayofmonth($"date_parsed"))
      .withColumn(
        "period",
        when($"day" <= 10, "early")
          .when($"day" <= 20, "mid")
          .otherwise("late")
      )

    // 统计各时间段次数
    val countDF = periodDF
      .groupBy("Coupon_id")
      .pivot("period", Seq("early", "mid", "late"))
      .count()
      .na.fill(0)

    // 计算概率（严格归一）
    val resultDF = countDF
      .withColumn("total", $"early" + $"mid" + $"late")
      .filter($"total" > 0)
      .select(
        $"Coupon_id",
        // 保留 3 位小数（有限小数不会被强制补零）
        round($"early" / $"total", 3).as("early_prob"),
        round($"mid" / $"total", 3).as("mid_prob"),
        round($"late" / $"total", 3).as("late_prob")
      )
      .orderBy($"Coupon_id")

    // 控制台展示前 10 行
    println("===== Task 2.1 优惠券使用时间分布（前 10 行）=====")
    resultDF.show(10, truncate = false)

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
