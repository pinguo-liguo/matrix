package org.apache.spark.core.context

import org.apache.spark.core.utils.ContextUtils
import org.apache.spark.framework.scheduler.{ContextLike, SparkJob, SparkJobBase}
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.slf4j.LoggerFactory

/**
 * This is a default implementation for Spark Context factory.
 */
class SparkContextFactory extends JobContextFactory {
  type C = SparkContext with ContextLike
  val logger = LoggerFactory.getLogger(getClass)
  
  def makeContext(config: Config, contextName: String) = {
    val sparkConf = ContextUtils.configToSparkConf(config, contextName)
    logger.info(s"Creating Spark context $contextName with config $sparkConf.")
    new SparkContext(sparkConf) with ContextLike {
      val contextClass = classOf[SparkContext].getName
      def sparkContext: SparkContext = this.asInstanceOf[SparkContext]
      def isValidJob(job: SparkJobBase) = job.isInstanceOf[SparkJob]
    }
  }
}
