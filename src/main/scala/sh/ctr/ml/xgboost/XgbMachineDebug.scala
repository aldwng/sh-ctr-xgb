package sh.ctr.ml.xgboost

import sh.ctr.common.Job
import sh.ctr.common.PathUtils.getCustomPath
import ml.dmlc.xgboost4j.scala.spark.XGBoost
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils

// this example works with Iris dataset (https://archive.ics.uci.edu/ml/datasets/iris)

class XgbMachineDebug() extends Job(platform = "app") {

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

    val debugRdd = trainLps
      .map {
        lp =>
          lp.label + "\t" + lp.features.size
      }

    val numRound = 10

    val params = List(
      "eta" -> 1F,
      "max_depth" -> 5,
      "objective" -> "binary:logistic",
      "lambda" -> 2.5
    ).toMap
    println(params)

//    val xgbm = XGBoost.trainWithRDD(trainLps, params, numRound,
//      20, obj = null, eval = null, useExternalMemory = false, Float.NaN)

//    val predicted = xgbm.predict(testVectors)

    val debugPath = gbmPath + "/debug"
    getFileSystem.delete(new Path(debugPath), true)
    debugRdd.repartition(5).saveAsTextFile(debugPath)
  }
}