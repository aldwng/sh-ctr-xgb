package sh.ctr.common

object Maths {

  def sampleFactor(days: Double): Double = {
    1D / Math.pow(days, 0.25)
  }

  def wilsonScoreInterval(denominator: Long, numerator: Long, z: Double = 1.96): (Double, Double) = {
    if (numerator * denominator == 0 || denominator < numerator) {
      return (0D, 0D)
    }
    val n = numerator
    val m = denominator
    val p = n.toDouble / m
    val lowerScore = (p + (z * z) / (2D * n) - z * Math.sqrt((p * (1D - p) + (z * z) / (4D * n)) / n)) /
      (1D + (z * z) / n)
    val upperScore = (p + (z * z) / (2D * n) + z * Math.sqrt((p * (1D - p) + (z * z) / (4D * n)) / n)) /
      (1D + (z * z) / n)
    (lowerScore, upperScore)
  }
}
