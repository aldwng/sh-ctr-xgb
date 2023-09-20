package sh.ctr.ml.xgboost

import sh.ctr.common.Job
import sh.ctr.common.PathUtils.getCustomPath
import ml.dmlc.xgboost4j.scala.spark.{XGBoost, XGBoostClassifier}
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils

// this example works with Iris dataset (https://archive.ics.uci.edu/ml/datasets/iris)

class XgbMachineRdd() extends Job(platform = "app") {

  def process(dt: String): Unit = {

    val spark = client
    val sc = spark.sparkContext

    val gbmPath = getCustomPath("app", dt, "gbmFeature", "518")
    val tranDsPath = gbmPath + "/trainDs"
    val testDsPath = gbmPath + "/testDs"

    val trainRdd = MLUtils.loadLibSVMFile(sc, tranDsPath)
    val testRdd = MLUtils.loadLibSVMFile(sc, testDsPath)

    val trainLps = trainRdd.map {
      lp => {
        val features = lp.features.toArray
        val label = lp.label
        LabeledPoint(label, Vectors.dense(features))
      }
    }

    val testVectors = testRdd.map {
      lp => {
        val features = lp.features.toArray
        val label = lp.label
        Vectors.dense(features)
      }
    }

    val numRound = 3

    val params = List(
      "tree_method" -> "hist",
      "eta" -> 1F,
      "max_depth" -> 4,
      "objective" -> "binary:logistic",
      "lambda" -> 1.0
    ).toMap
    println(params)

//    val xgbm = XGBoost.trainWithRdd(trainLps, params, numRound,
//      20, obj = null, eval = null, useExternalMemory = false, Float.NaN)

//    val predicted = xgbm.predict(testVectors)

//    val predictionPath = gbmPath + "/prediction"
//    getFileSystem.delete(new Path(predictionPath), true)
//    predicted.repartition(5).saveAsTextFile(predictionPath)
  }
}