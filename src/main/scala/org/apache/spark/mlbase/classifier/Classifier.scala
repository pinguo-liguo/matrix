package org.apache.spark.mlbase.classifier

trait Classifier {
  
  
  def train():Model

}