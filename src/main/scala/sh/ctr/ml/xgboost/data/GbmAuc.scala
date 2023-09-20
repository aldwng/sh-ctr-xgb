package sh.ctr.ml.xgboost.data

import sh.ctr.common.Job
import sh.ctr.common.PathUtils.getCustomPath

class GbmAuc(platform: String) extends Job(platform) {

  def calcAuc(results: Array[(Float, Double)]): Double = {
    val sorted = results.sortWith(_._2 < _._2)
    var pos = 0L
    var neg = 0L
    var sumRank = 0L
    for (i <- sorted.indices) {
      if (sorted(i)._1 > 0F) {
        pos += 1L
        sumRank += i
      } else {
        neg += 1L
      }
    }
    val auc = (sumRank - pos.toDouble * (pos + 1) / 2) / (pos * neg)
    auc
  }

  def process(date: String): Unit = {
    val sc = client.sparkContext

    val ascPath = getCustomPath(platform, date, "gbmFeature", "518")
    val readPath = ascPath + "/try_1694764661621"
    println("read path: " + readPath)

    val results = sc.textFile(readPath).map {
      line => {
        val cleanLine = line.replaceAll("\\[", "").replaceAll("]", "")
        val digits = cleanLine.split(",")
        (digits.head.toFloat, digits(2).toDouble)
      }
    }.collect()

    val auc = calcAuc(results)
    val outRdd = sc.parallelize(Seq("test auc: " + auc))

    val aucPath = ascPath + "/try_auc"
    outRdd.repartition(1).saveAsTextFile(aucPath)
  }
}
