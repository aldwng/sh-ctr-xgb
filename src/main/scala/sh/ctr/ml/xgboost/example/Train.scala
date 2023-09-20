package sh.ctr.ml.xgboost.example

//import ml.dmlc.xgboost4j.scala.spark.XGBoostClassifier
//import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
//
//object Train {
//
//  def main(args: Array[String]): Unit = {
//    if (args.length < 1) {
//      // scalastyle:off
//      println("Usage: program input_path")
//      sys.exit(1)
//    }
//    val spark = SparkSession.builder().getOrCreate()
//    val inputPath = args(0)
//    val schema = new StructType(Array(
//      StructField("sepal length", DoubleType, true),
//      StructField("sepal width", DoubleType, true),
//      StructField("petal length", DoubleType, true),
//      StructField("petal width", DoubleType, true),
//      StructField("class", StringType, true)))
//    val rawInput = spark.read.schema(schema).csv(inputPath)
//
//    // transform class to index to make xgboost happy
//    val stringIndexer = new StringIndexer()
//      .setInputCol("class")
//      .setOutputCol("classIndex")
//      .fit(rawInput)
//    val labelTransformed = stringIndexer.transform(rawInput).drop("class")
//    // compose all feature columns as vector
//    val vectorAssembler = new VectorAssembler().
//      setInputCols(Array("sepal length", "sepal width", "petal length", "petal width")).
//      setOutputCol("features")
//    val xgbInput = vectorAssembler.transform(labelTransformed).select("features",
//      "classIndex")
//
//    val Array(train, eval1, eval2, test) = xgbInput.randomSplit(Array(0.6, 0.2, 0.1, 0.1))
//
//    /**
//     * setup  "timeout_request_workers" -> 60000L to make this application if it cannot get enough resources
//     * to get 2 workers within 60000 ms
//     *
//     * setup "checkpoint_path" -> "/checkpoints" and "checkpoint_interval" -> 2 to save checkpoint for every
//     * two iterations
//     */
//    val xgbParam = Map("eta" -> 0.1f,
//      "max_depth" -> 2,
//      "objective" -> "multi:softprob",
//      "num_class" -> 3,
//      "num_round" -> 100,
//      "num_workers" -> 2,
//      "eval_sets" -> Map("eval1" -> eval1, "eval2" -> eval2))
//    val xgbClassifier = new XGBoostClassifier(xgbParam).
//      setFeaturesCol("features").
//      setLabelCol("classIndex")
//    val xgbClassificationModel = xgbClassifier.fit(train)
//    val results = xgbClassificationModel.transform(test)
//    results.show()
//  }
//}
