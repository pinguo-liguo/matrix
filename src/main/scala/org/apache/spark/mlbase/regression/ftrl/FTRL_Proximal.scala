package org.apache.spark.mlbase.regression.ftrl

import math._


/**
 * Follow the regularized leader-proximal
 * Reference: "Ad click Prediction: A View from the Trenches"
 *
 * @param alpha depend on the features and dataset
 * @param beta beta=1 is usually good enough
 * @param L1
 * @param L2
 * @param D feature dimension
 * @param interaction interaction feature
 */
class FTRL_Proximal(alpha: Double = 0.1, beta: Double = 0.1, L1: Double = 0.0, L2: Double = 0.0,
                    D: Int = 1, interaction: Boolean = true)
  extends Learner {

  var z = Array[Double](D)
  var n = Array[Double](D)
  var w = Array[Double](D)

  /**
   * Get probability estimation on x
   *
   * @param x features
   * @return probability of p(y=1|x;w)
   */
  def predict(x: Array[(Int, Double)]): Double = {
    var wTx : Double = 0.0
    for ((i, value) <- x) {
      val sign = if (z(i) < 0) 1 else -1

      if (sign * z(i) <= L1) {
        w(i) = 0.0
      }
      else {
        w(i)  = (sign * L1 - z(i)) / (beta + sqrt(n(i)) / alpha + L2)
      }
      wTx += w(i)
    }
    // bounded sigmoid function
    1.0 / (1.0 + exp(max(min(wTx, 35.0), -35.0)))
  }

  /**
   * Update z and n using x,p,y
   *
   * @param x features, a list of (index, value)
   * @param p click probability prediction of our model
   * @param y answer
   */
  def update(x: Array[(Int, Double)], p: Double, y: Int) {
    val g = p - y
    for ((i, value) <- x) {
      val sigma = (sqrt(n(i) + g*g) - sqrt(n(i))) / alpha
      z(i) += g - sigma * w(i)
      n(i) += g * g
    }
  }

}