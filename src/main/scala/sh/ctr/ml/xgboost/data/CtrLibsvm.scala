package sh.ctr.ml.xgboost.data

import sh.ctr.common.Maths.sampleFactor
import sh.ctr.common.PathUtils.{getCustomPath, getFeaturePath}
import sh.ctr.common.Utils.DATE_FORMAT_FULL
import sh.ctr.common.{Job, Utils}
import sh.ctr.ml.xgboost.data.GbmSchema.CTR_CATEGORIES
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.storage.StorageLevel

import java.util.Date

class CtrLibsvm(platform: String, trainDays: Int = 30, testDays: Int = 1) extends Job(platform) {

  private def getValidPath(date: String, minusDay: Int): String = {
    val fs = getFileSystem
    val day = Utils.someDay(date, -minusDay)
    val path = getFeaturePath(platform, day, "518")
    val successFile = new Path(path + "/_SUCCESS")
    if (fs.exists(successFile)) {
      return path
    }
    StringUtils.EMPTY
  }

  private def appendPath(path: String, subDir: String): String = {
    path + "/" + subDir
  }

  def process(date: String): Unit = {
    val sc = client.sparkContext

    val ascPath = getCustomPath(platform, date, "ctrLibsvm", "518")
    getFileSystem.delete(new Path(ascPath), true)
    println("output path: " + ascPath)

    val curTimeBc = sc.broadcast[Long](new Date().getTime)

    val unionFeatureRdd = sc.union(Range(testDays, testDays + trainDays).map {
      minusDay => {
        val inputPath = getValidPath(date, minusDay)
        if (StringUtils.isBlank(inputPath)) {
          return
        }

        sc.textFile(inputPath).map {
          line => {
            val Array(target, datetime, trunk) = line.split("\t", 3)

            val lrFeatures = trunk.split("\t").filter(x => !x.contains("$")).map {
              fea =>
                val feaCat = fea.split(":")(0).split("&")
                if (feaCat.length > 1) {
                  val cat = feaCat(0)
                  val catVal = feaCat(1)
                  cat -> catVal
                } else {
                  feaCat.head -> "AIOOBE"
                }
            }.filter(x => CTR_CATEGORIES.contains(x._1))

            try {
              val day = DATE_FORMAT_FULL.parse(datetime)
              val apart = Math.ceil((curTimeBc.value - day.getTime) / 86400000.0).toInt
              if (apart > 0 && ("1".equals(target) || Math.random() < sampleFactor(apart))) {
                (target, apart, lrFeatures)
              } else {
                null
              }
            } catch {
              case _: Exception =>
                null
            }
          }
        }.filter(_ != null)
      }
    }).persist(StorageLevel.MEMORY_AND_DISK)

    val featureMap = unionFeatureRdd
      .flatMap {
        case (target, apart, cats) =>
          cats.map {
            case (cat, catVal) =>
              var count = 1D
              if ("0".equals(target)) {
                count = 1D / sampleFactor(apart)
              }
              (cat + "=" + catVal) -> count
          }
      }
      .reduceByKey(_ + _)
      .filter(_._2 > 20D)
      .sortBy(_._2, ascending = false, 1)
      .zipWithIndex()
      .map {
        catIndex =>
          catIndex._1._1 + "\t" + (catIndex._2 + 1) + "\t" + catIndex._1._2
      }

    val featureMapPath = appendPath(ascPath, "featureMap")
    featureMap.repartition(1).saveAsTextFile(featureMapPath)

    val readInFeatureMap = sc.textFile(featureMapPath)
      .map {
        line =>
          val fields = line.split("\t")
          fields(0) -> fields(1)
      }
    val featureMapBc = sc.broadcast(readInFeatureMap.collectAsMap())

    val trainDs = unionFeatureRdd
      .map {
        case (target, _, cats) =>
          val content = cats.map {
            case (cat, catVal) =>
              val name = cat + "=" + catVal
              featureMapBc.value.getOrElse(name, "-1").toLong
          }.filter(_ > 0).sortWith(_ < _).map {
            id =>
              id + ":1.0"
          }.mkString(" ")
          target + " " + content
      }

    val trainDsPath = appendPath(ascPath, "trainDs")
    trainDs.repartition(200).saveAsTextFile(trainDsPath)

    val testDs = sc.union(Range(0, testDays).map {
      minusDay => {
        val inputPath = getValidPath(date, minusDay)
        if (StringUtils.isBlank(inputPath)) {
          return
        }
        sc.textFile(inputPath).map {
          line => {
            val Array(target, datetime, trunk) = line.split("\t", 3)

            val lrFeatures = trunk.split("\t").filter(x => !x.contains("$")).map {
              fea =>
                val feaCat = fea.split(":")(0).split("&")
                if (feaCat.length > 1) {
                  val cat = feaCat(0)
                  val catVal = feaCat(1)
                  cat -> catVal
                } else {
                  feaCat.head -> "AIOOBE"
                }
            }.filter(x => CTR_CATEGORIES.contains(x._1))

            (target, datetime, lrFeatures)
          }
        }
      }
    })
      .map {
        case (target, _, cats) =>
          val content = cats.map {
            case (cat, catVal) =>
              val name = cat + "=" + catVal
              featureMapBc.value.getOrElse(name, "-1").toLong
          }.filter(_ > 0).sortWith(_ < _).map {
            id =>
              id + ":1.0"
          }.mkString(" ")
          target + " " + content
      }

    val testDsPath = appendPath(ascPath, "testDs")
    testDs.repartition(200).saveAsTextFile(testDsPath)
  }
}
