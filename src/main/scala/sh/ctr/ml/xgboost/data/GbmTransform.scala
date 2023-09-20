package sh.ctr.ml.xgboost.data

import sh.ctr.common.PathUtils.{getCustomPath, getFeaturePath}
import sh.ctr.common.{Job, Utils}
import sh.ctr.ml.xgboost.data.GbmSchema.CTR_CATEGORIES
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.storage.StorageLevel

class GbmTransform(platform: String) extends Job(platform) {

  private val TRAIN_DS_DAY_SPAN = 30
  private val TEST_DS_DAY_SPAN = 1

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

    val ascPath = getCustomPath(platform, date, "gbmFeature", "518")
    getFileSystem.delete(new Path(ascPath), true)
    println("output path: " + ascPath)

    val unionFeatureRdd = sc.union(Range(TEST_DS_DAY_SPAN, TEST_DS_DAY_SPAN + TRAIN_DS_DAY_SPAN).map {
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
                val feaP = fea.split(":")
                val feaCat = feaP(0).split("&")
                val feaVal = feaP(1)

                if (feaCat.length > 1) {
                  val cat = feaCat(0)
                  val catVal = feaCat(1)
                  if (cat.contains("ctr")) {
                    cat -> (feaVal, true)
                  } else {
                    cat -> (catVal, false)
                  }
                } else {
                  feaCat.head -> ("AIOOBE", false)
                }
            }.filter(x => CTR_CATEGORIES.contains(x._1))

            (target, datetime, lrFeatures)
          }
        }
      }
    }).persist(StorageLevel.MEMORY_AND_DISK)

    val featureMap = unionFeatureRdd
      .flatMap {
        case (_, _, cats) =>
          cats.map {
            case (cat, (catVal, isNumeric)) =>
              if (isNumeric) {
                cat -> 1L
              } else {
                (cat + "=" + catVal) -> 1L
              }
          }
      }
      .reduceByKey(_ + _)
      .filter(_._2 > 30L)
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
            case (cat, (catVal, isNumeric)) =>
              var name = cat
              var idVal = catVal
              if (!isNumeric) {
                idVal = "1.0"
                name = cat + "=" + catVal
              }
              (featureMapBc.value.getOrElse(name, "-1").toLong, idVal)
          }.filter(_._1 > 0).sortWith(_._1 < _._1).map {
            case (id, idVal) =>
              id + ":" + idVal
          }.mkString(" ")
          target + " " + content
      }

    val trainDsPath = appendPath(ascPath, "trainDs")
    trainDs.repartition(200).saveAsTextFile(trainDsPath)

    val testDs = sc.union(Range(0, TEST_DS_DAY_SPAN).map {
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
                val feaP = fea.split(":")
                val feaCat = feaP(0).split("&")
                val feaVal = feaP(1)

                if (feaCat.length > 1) {
                  val cat = feaCat(0)
                  val catVal = feaCat(1)
                  if (cat.contains("ctr")) {
                    cat -> (feaVal, true)
                  } else {
                    cat -> (catVal, false)
                  }
                } else {
                  feaCat.head -> ("AIOOBE", false)
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
            case (cat, (catVal, isNumeric)) =>
              var name = cat
              var idVal = catVal
              if (!isNumeric) {
                idVal = "1.0"
                name = cat + "=" + catVal
              }
              (featureMapBc.value.getOrElse(name, "-1").toLong, idVal)
          }.filter(_._1 > 0).sortWith(_._1 < _._1).map {
            case (id, idVal) =>
              id + ":" + idVal
          }.mkString(" ")
          target + " " + content
      }

    val testDsPath = appendPath(ascPath, "testDs")
    testDs.repartition(200).saveAsTextFile(testDsPath)
  }
}
