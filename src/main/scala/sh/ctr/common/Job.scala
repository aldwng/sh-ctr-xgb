package sh.ctr.common

import Constant.BASE_PATH_TEST
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession

import java.net.URI

abstract class Job(platform: String) {
  protected lazy val client: SparkSession = Container.sparkSession()

  // dt is the date param
  def process(dt: String): Unit

  protected def getFileSystem: FileSystem = {
    FileSystem.get(new URI(BASE_PATH_TEST), client.sparkContext.hadoopConfiguration)
  }
}