package sh.ctr.ml.xgboost

import sh.ctr.common.Job
import sh.ctr.common.PathUtils.getCustomPath
import ml.dmlc.xgboost4j.scala.spark.XGBoostClassifier
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.sql.functions.col
import org.apache.spark.storage.StorageLevel


// So-far-so-good parameter settings:
// numWorkers=10, numThreads=10
// maxDepth=10, minChildWeight=7,
// numRounds=350, eta=0.3
// subSample=0.8, colSampleByTree=0.8,
// maxDeltaStep=5

class XgbMachine(nWorkers: Int, nThreads: Int,
                 maxDepth: Int, minChildWeight: Int,
                 numRounds: Int, eta: Double,
                 subSample: Double, colSample: Double,
                 maxDeltaStep: Double) extends Job(platform = "app") {

  def process(dt: String): Unit = {

    val spark = client

    val gbmPath = getCustomPath("app", dt, "gbmFeature", "518")
    val tranDsPath = gbmPath + "/trainDs"
    val testDsPath = gbmPath + "/testDs"

    val trainDfInput = spark.read.format("libsvm").load(tranDsPath).toDF("label", "features")
      .repartition(200).persist(StorageLevel.MEMORY_AND_DISK)
    val testDfInput = spark.read.format("libsvm").load(testDsPath).toDF("label", "features")
      .repartition(200).persist(StorageLevel.MEMORY_AND_DISK)

    val params = Map(
      "verbosity" -> 2
    )

    val xgbc = new XGBoostClassifier(params)
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setTreeMethod("approx")
      .setMaxDepth(maxDepth)
      .setObjective("binary:logistic")
      .setEta(eta)
      .setSubsample(subSample)
      .setColsampleBytree(colSample)
      .setMinChildWeight(minChildWeight)
      .setNumWorkers(nWorkers)
      .setNthread(nThreads)
      .setNumRound(numRounds)
      .setMaxDeltaStep(maxDeltaStep)

    val xgbm = xgbc.fit(trainDfInput)

    val trying = xgbm.transform(testDfInput)

    val evaluator = new BinaryClassificationEvaluator()
    val auc = evaluator.evaluate(trying)
    println("test auc is ", auc)

    val simplyTry = trying.select(col("label"), col("probability")).rdd
    val predictionPath = gbmPath + "/" + maxDepth + "_" + minChildWeight + "_" + numRounds + "_" + eta +
      "_" + subSample + "_" + colSample + "_" + maxDeltaStep
    getFileSystem.delete(new Path(predictionPath), true)
    simplyTry.repartition(10).saveAsTextFile(predictionPath)

    val evalPath = predictionPath + "_eval"
    getFileSystem.delete(new Path(evalPath), true)
    spark.sparkContext.parallelize(Seq("test eval:" + auc)).repartition(1).saveAsTextFile(evalPath)
  }
}