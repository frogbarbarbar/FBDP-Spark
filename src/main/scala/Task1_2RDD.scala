import org.apache.spark.SparkContext

object Task1_2RDD {

  def main(args: Array[String]): Unit = {

    val sc = SparkContext.getOrCreate()

    val inputPath = "file:///home/hadoop/projects/lab4/data/ccf_online_stage1_train.csv"

    val outputPath = "file:///home/hadoop/projects/lab4/outputs/task1/online_consumption_table"

    val raw = sc.textFile(inputPath)

    val header = raw.first()
    val data = raw.filter(_ != header)

    // User_id, Merchant_id, Action, Coupon_id, Discount_rate, Date_received, Date
    val merchantSamples = data
      .map(_.split(","))
      .filter(_.length >= 7)
      .map { fields =>
        val merchantId = fields(1)
        val couponId = fields(3)
        val date = fields(6)

        val sampleType =
          if (date == "null" && couponId != "null") {
            "negative"
          } else if (date != "null" && couponId == "null") {
            "normal"
          } else if (date != "null" && couponId != "null") {
            "positive"
          } else {
            "other"
          }

        ((merchantId, sampleType), 1)
      }
      .filter(_._1._2 != "other")
      .reduceByKey(_ + _)

    val merchantStat = merchantSamples
      .map {
        case ((merchantId, sampleType), cnt) =>
          sampleType match {
            case "negative" => (merchantId, (cnt, 0, 0))
            case "normal"   => (merchantId, (0, cnt, 0))
            case "positive" => (merchantId, (0, 0, cnt))
          }
      }
      .reduceByKey((a, b) =>
        (a._1 + b._1, a._2 + b._2, a._3 + b._3)
      )
      .sortByKey(true)

    merchantStat
      .map {
        case (merchantId, (neg, normal, pos)) =>
          s"$merchantId\t$neg\t$normal\t$pos"
      }
      .saveAsTextFile(outputPath)

    println("Merchant_id\tNegative\tNormal\tPositive")
    merchantStat.take(10).foreach {
      case (merchantId, (neg, normal, pos)) =>
        println(s"$merchantId\t$neg\t$normal\t$pos")
    }
  }
}
