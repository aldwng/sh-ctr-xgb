package sh.ctr.ml

import sh.ctr.common.Constant.PLATFORM_APP
import sh.ctr.common.{Container, Utils}
import sh.ctr.ml.xgboost.data.{CtrLibsvm, GbmAuc, GbmTransform}
import sh.ctr.ml.xgboost.{XgbMachine, XgbMachineDebug}

object Machines {

  def main(args: Array[String]): Unit = {
    val options = Utils.parseArgs(args)
    val mach = options.getOrElse("--machine", "xgb")
    val cmd = options.getOrElse("--command", "dataTransform")
    val date = options.getOrElse("--date", Utils.yesterday())
    var days = options.getOrElse("--days", 7).toString.toInt
    val platform = options.getOrElse("--platform", PLATFORM_APP).toLowerCase
    val xNWorkers = options.getOrElse("--nWorkers", "1").toInt
    val xNThreads = options.getOrElse("--nThreads", "1").toInt
    val xMaxDepth = options.getOrElse("--maxDepth", "2").toInt
    val xMinChildWeight = options.getOrElse("--minChildWeight", "1").toInt
    val xNumRounds = options.getOrElse("--numRounds", "10").toInt
    val xEta = options.getOrElse("--eta", "0.3").toDouble
    val xSubSample = options.getOrElse("--subSample", "0.8").toDouble
    val xColSample = options.getOrElse("--colSample", "0.8").toDouble
    val xMaxDeltaStep = options.getOrElse("--maxDeltaStep", "0").toDouble

    if (days < 1) {
      days = 7
    }

    mach match {
      case "xgb" =>
        cmd match {
          case "dataTransform" =>
            val gt = new GbmTransform(platform)
            for (i <- 0 until days) {
              gt.process(Utils.someDay(date, -i))
            }
          case "ctrLibsvm" =>
            val cl = new CtrLibsvm(platform)
            for (i <- 0 until days) {
              cl.process(Utils.someDay(date, -i))
            }
          case "xgbmachine" =>
            val gt = new XgbMachine(xNWorkers, xNThreads, xMaxDepth, xMinChildWeight, xNumRounds,
              xEta, xSubSample, xColSample, xMaxDeltaStep)
            for (i <- 0 until days) {
              gt.process(Utils.someDay(date, -i))
            }
          case "machineDebug" =>
            val gt = new XgbMachineDebug()
            for (i <- 0 until days) {
              gt.process(Utils.someDay(date, -i))
            }
          case "machineAuc" =>
            val ga = new GbmAuc(platform)
            for (i <- 0 until days) {
              ga.process(Utils.someDay(date, -i))
            }
          case _ =>
            println("command not found!")
        }
      case _ =>
        println("ml machine not found!")
    }

    Container.close()
  }
}
