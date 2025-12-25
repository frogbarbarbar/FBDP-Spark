import org.apache.spark.SparkContext

object Task1_1RDD {

  def main(args: Array[String]): Unit = {

    // spark-shell 中：sc 已经存在，直接用
    val sc = SparkContext.getOrCreate()

    // 输入 / 输出路径 ———— 用绝对的Local路径，否则会被默认为HDFS路径
    val inputPath = "file:///home/hadoop/projects/lab4/data/ccf_online_stage1_train.csv"
    val outputPath = "file:///home/hadoop/projects/lab4/outputs/task1/task1-1-coupon-usage"

    // 读取 CSV
    val raw = sc.textFile(inputPath)

    // 去掉表头
    val header = raw.first()
    val data = raw.filter(_ != header)

    // 字段顺序：User_id, Merchant_id, Action, Coupon_id, Discount_rate, Date_received, Date
    val couponUsed = data
      .map(_.split(","))
      .filter(fields =>
        fields(3) != "null" && fields(6) != "null"
      )
      .map(fields => (fields(3), 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)

    // 保存完整结果
    couponUsed
      .map { case (couponId, cnt) => s"$couponId\t$cnt" }
      .saveAsTextFile(outputPath)

    // 控制台输出 Top10
    println("Top 10 coupon usage:")
    couponUsed.take(10).foreach(println)
  }
}