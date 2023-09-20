package sh.ctr.common

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object Container {
  private var ss: SparkSession = _

  def sparkSession(): SparkSession = {
    if (null == ss) {
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      ss = SparkSession.builder()
        .appName("sh-ctr-spark—job")
        .enableHiveSupport()
        .getOrCreate()

      ss.sparkContext.setLogLevel("INFO")
    }
    ss
  }

  def sparkContext(): SparkContext = {
    sparkSession().sparkContext
  }

  def close(): Unit = {
    if (null != ss) {
      ss.close()
    }
  }
}
