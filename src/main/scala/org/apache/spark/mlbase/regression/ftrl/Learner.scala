/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
