package sh.ctr.ml.xgboost

import sh.ctr.common.Job
import sh.ctr.common.PathUtils.getCustomPath
import ml.dmlc.xgboost4j.scala.spark.XGBoost
import org.apache.hadoop.fs.Path
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.Row

// this example works with Iris dataset (https://archive.ics.uci.edu/ml/datasets/iris)

class XgbMachineDf() extends Job(platform = "app") {

  def process(dt: String): Unit = {

    val spark = client

    val gbmPath = getCustomPath("app", "20230904", "gbmFeature", "518")
    val tranDsPath = gbmPath + "/trainDs"
    val testDsPath = gbmPath + "/testDs"

    val trainDsInput = spark.read.format("libsvm").load(tranDsPath).toDF("label", "feature")
    val testDsInput = spark.read.format("libsvm").load(testDsPath).toDF("label", "feature")

    val numRound = 20

    val params = List(
      "eta" -> 1F,
      "max_depth" -> 6,
      "objective" -> "binary:logistic",
      "lambda" -> 2.5
    ).toMap
    println(params)

//    val xgbm = XGBoost.trainWithDataFrame(trainDsInput, params, numRound,
//      45, obj = null, eval = null,
//      useExternalMemory = false, Float.NaN, "feature", "label")
//
//    val predicted = xgbm.transform(testDsInput)
//
//    val scoreAndLabels = predicted.select(xgbm.getPredictionCol, xgbm.getLabelCol)
//      .rdd
//      .map {
//        case Row(score: Double, label: Double) =>
//          (score, label)
//      }
//
//    val metric = new BinaryClassificationMetrics(scoreAndLabels)
//    val auc = metric.areaUnderROC()
//    println("auc: " + auc)
//
//    val predictionPath = gbmPath + "/prediction"
//    getFileSystem.delete(new Path(predictionPath), true)
//    scoreAndLabels.repartition(1).saveAsTextFile(predictionPath)
  }
}