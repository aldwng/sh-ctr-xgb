package sh.ctr.common

import org.apache.commons.lang3.StringUtils
import sh.ctr.common.Constant.BASE_PATH_XGB
import sh.ctr.common.Utils.DATE_FORMAT

import java.text.SimpleDateFormat

object PathUtils {

  private def getBasePath(model: String): String = {
    BASE_PATH_XGB + "_" + model
  }

  def getLabelPath(platform: String, date: String, model: String): String = {
    "%s/%s/label/%s".format(
      getBasePath(model),
      platform,
      new SimpleDateFormat("yyyy/MM/dd").format(DATE_FORMAT.parse(date).getTime)
    )
  }

  def getFeaturePath(platform: String, date: String, model: String): String = {
    "%s/%s/feature/%s".format(
      getBasePath(model),
      platform,
      new SimpleDateFormat("yyyy/MM/dd").format(DATE_FORMAT.parse(date).getTime)
    )
  }

  def getTrainPath(platform: String, date: String, days: Int, model: String, version: String = ""): String = {
    "%s/%s/train/%s_%d_%s".format(
      getBasePath(model),
      platform,
      date,
      days,
      version
    )
  }

  def getEvaluatePath(platform: String, date: String, days: Int, model: String, version: String = ""): String = {
    "%s/%s/evaluate/%s_%d_%s".format(
      getBasePath(model),
      platform,
      date,
      days,
      version
    )
  }

  def getCustomPath(platform: String, date: String, tag: String, model: String,
                    version: String = ""): String = {
    if (StringUtils.isBlank(version)) {
      return "%s/%s/%s/%s".format(
        getBasePath(model),
        platform,
        tag,
        date
      )
    }
    "%s/%s/%s/%s_%s".format(
      getBasePath(model),
      platform,
      tag,
      date,
      version
    )
  }

}
