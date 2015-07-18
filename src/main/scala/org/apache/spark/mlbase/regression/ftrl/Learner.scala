package org.apache.spark.mlbase.regression.ftrl

import math._

abstract class Learner {

  def predict(x: Array[(Int, Double)]): Double

  def update(x: Array[(Int, Double)], p: Double, y: Int)

  /**
   * logarithmic loss of p given y
   *
   * @param p our prediction
   * @param y real answer
   * @return
   */
  def logLoss(p: Double, y: Double): Double = {
    val t = max(min(p, 1.0 - 10e-15), 10e-15)
    if (y==1) -log(t) else -log(1.0-t)
  }

}


case class LabeledPoint(label: Double, features: Array[(Int, Double)])
