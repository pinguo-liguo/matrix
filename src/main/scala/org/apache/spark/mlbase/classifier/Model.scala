package org.apache.spark.mlbase.classifier

import org.apache.spark.mlbase.core.Instances

trait Model {

  def predict(test:Instances):Double
  
}