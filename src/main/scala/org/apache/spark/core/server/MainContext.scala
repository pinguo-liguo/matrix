package org.apache.spark.core.server

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import org.apache.spark.core.logging.LoggingOutputStream
import org.apache.spark.core.server.actors.ContextActor
import org.apache.spark.core.utils.ActorUtils
import org.slf4j.LoggerFactory

/**
 * Spark context container entry point.
 */
object MainContext {

  LoggingOutputStream.redirectConsoleOutput
  val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]) {
    val contextName = args(0)
    val port = args(1).toInt

    log.info(s"Started new process for contextName = $contextName with port = $port")

    val defaultConfig = ConfigFactory.load("deploy").withFallback(ConfigFactory.load())
    val config = ActorUtils.remoteConfig("localhost", port, defaultConfig)
    val system = ActorSystem(ActorUtils.PREFIX_CONTEXT_SYSTEM + contextName, config)

    system.actorOf(Props(new ContextActor(defaultConfig)), ActorUtils.PREFIX_CONTEXT_ACTOR + contextName)

    log.info(s"Initialized system ${ActorUtils.PREFIX_CONTEXT_SYSTEM}$contextName and actor ${ActorUtils.PREFIX_CONTEXT_SYSTEM}$contextName")
  }
}
