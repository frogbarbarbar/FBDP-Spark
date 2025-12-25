import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.functions.vector_to_array


object Task3_MLlib {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Task3_Coupon_Prediction")
      .master("local[*]")
      .config("spark.sql.session.timeZone", "GMT+8")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    // ========================
    // 1. 读取训练数据
    // ========================
    val TRAIN_PATH = "file:///home/hadoop/projects/lab4/data/ccf_offline_stage1_train.csv"

    val rawDF = spark.read
      .option("header", "true")
      .option("nullValue", "null")
      .csv(TRAIN_PATH)

    // 只保留“领券记录”（过滤 null 和 "null"）
    val couponDF = rawDF.filter(
      $"Coupon_id".isNotNull && $"Coupon_id" =!= "null" &&
      $"Date_received".isNotNull && $"Date_received" =!= "null"
    )

    // ========================
    // 2. 构造标签 label
    // label = 1：15天内使用
    // label = 0：未使用或超期使用
    // ========================
    val labeledDF = couponDF.withColumn(
        "label",
        when(
          $"Date".isNotNull && $"Date" =!= "null" &&
          datediff(
            to_date($"Date", "yyyyMMdd"),
            to_date($"Date_received", "yyyyMMdd")
          ).between(0, 15),
          1
        ).otherwise(0)
    )

    // ========================
    // 3. 特征工程
    // ========================

    // 3.1 折扣率特征（0~1）
    val discountUDF = udf { (rate: String) =>
      if (rate == null || rate == "null") {
        0.0
      } else if (rate.contains(":")) {
        val parts = rate.split(":")
        val full = parts(0).toDouble
        val minus = parts(1).toDouble
        (full - minus) / full
      } else {
        try {
          rate.toDouble
        } catch {
          case _: Exception => 0.0
        }
      }
    }

    // 3.2 距离特征
    val distanceCol =
      when($"Distance".isNull || $"Distance" === "null", -1.0)
        .otherwise($"Distance".cast(DoubleType))

    // 3.3 领取时间 → 星期几
    val featureDF = labeledDF
      .withColumn("discount_rate", discountUDF($"Discount_rate"))
      .withColumn("distance", distanceCol)
      .withColumn(
        "receive_weekday",
        when($"Date_received".isNull || $"Date_received" === "null", -1)
          .otherwise(dayofweek(to_date($"Date_received", "yyyyMMdd")))
      )
      .select(
        $"label",
        $"discount_rate",
        $"distance",
        $"receive_weekday"
      )

    // ========================
    // 4. 组装特征向量
    // ========================
    val assembler = new VectorAssembler()
      .setInputCols(Array(
        "discount_rate",
        "distance",
        "receive_weekday"
      ))
      .setOutputCol("features")

    val finalDF = assembler.transform(featureDF)
      .select("label", "features")

    // ========================
    // 5. 划分训练 / 验证集
    // ========================
    val Array(trainDF, testDF) = finalDF.randomSplit(
      Array(0.8, 0.2),
      seed = 42
    )

    // ========================
    // 6. 训练 Logistic Regression
    // ========================
    val lr = new LogisticRegression()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxIter(50)
      .setRegParam(0.01)
      .setElasticNetParam(0.0)

    val model = lr.fit(trainDF)

    // ========================
    // 7. 模型评估（AUC）
    // ========================
    val predictions = model.transform(testDF)

    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setMetricName("areaUnderROC")

    val auc = evaluator.evaluate(predictions)

    println(s"===== Task3 Logistic Regression AUC = $auc =====")

    // ========================
    // 8. 读取测试集
    // ========================
    val TEST_PATH = "file:///home/hadoop/projects/lab4/data/ccf_offline_stage1_test_revised.csv"

    val testRawDF = spark.read
      .option("header", "true")
      .option("nullValue", "null")
      .csv(TEST_PATH)

    val testFeatureDF = testRawDF
      .filter(
        $"Coupon_id".isNotNull && $"Coupon_id" =!= "null" &&
        $"Date_received".isNotNull && $"Date_received" =!= "null"
      )
      .withColumn("discount_rate", discountUDF($"Discount_rate"))
      .withColumn(
          "distance",
          when($"Distance".isNull, -1.0)
            .otherwise($"Distance".cast(DoubleType))
      )
      .withColumn(
        "receive_weekday",
        when($"Date_received".isNull || $"Date_received" === "null", -1)
          .otherwise(dayofweek(to_date($"Date_received", "yyyyMMdd")))
      )

    val testFinalDF = assembler.transform(testFeatureDF)   // 组装特征向量

    // ========================
    // 9. 用训练好的模型做预测
    // ========================   
    val testPredictions = model.transform(testFinalDF)

    // ========================
    // 10. 导出预测结果
    // ======================== 
    val outputDF = testPredictions.select(
    $"User_id",
    $"Merchant_id",
    $"Coupon_id",
    $"Date_received",
    vector_to_array($"probability")(1).alias("use_prob"),
    $"prediction"
    )

    outputDF
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv("file:///home/hadoop/projects/lab4/outputs/task3/prediction_result")

    spark.stop()
  }
}
