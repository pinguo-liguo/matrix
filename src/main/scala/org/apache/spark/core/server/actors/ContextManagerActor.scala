package org.apache.spark.core.server.actors

import java.util

import akka.actor.{Actor, ActorRef, ActorSelection, Props}
import akka.pattern.ask
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.lang.exception.ExceptionUtils
import org.apache.spark.core.response.{Context, Contexts}
import ContextManagerActor.{IsAwake, NoSuchContext, ContextAlreadyExists}
import org.apache.spark.core.utils.ActorUtils
import org.slf4j.LoggerFactory
import ContextManagerActor._
import JarActor.{GetJarsPathForAll, ResultJarsPathForAll}

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.sys.process.{Process, ProcessBuilder}
import scala.util.Success

/**
 * Context management messages
 */
object ContextManagerActor {
  case class CreateContext(contextName: String, jars: String, config: Config)
  case class ContextInitialized(port: String)
  case class DeleteContext(contextName: String)
  case class ContextProcessTerminated(contextName: String, statusCode: Int)
  case class GetContext(contextName: String)
  case class GetContextInfo(contextName: String)
  case class GetAllContextsForClient()
  case class GetAllContexts()
  case class NoSuchContext()
  case class ContextAlreadyExists()
  case class DestroyProcess(process: Process)
  case class IsAwake()
  case class ContextInfo(contextName: String, sparkUiPort: String, @transient referenceActor: ActorSelection)
}

/**
 * Actor that creates, monitors and destroys contexts and corresponding processes.
 * @param defaultConfig configuration defaults
 * @param jarActor actor that responsible for jars which may be included to context classpath
 */
class ContextManagerActor(defaultConfig: Config, jarActor: ActorRef) extends Actor {

  val log = LoggerFactory.getLogger(getClass)

  var lastUsedPort = getValueFromConfig(defaultConfig, "appConf.actor.systems.first.port", 11000)
  var lastUsedPortSparkUi = getValueFromConfig(defaultConfig, "appConf.spark.ui.first.port", 16000)

  val contextMap = new mutable.HashMap[String, ContextInfo]() with mutable.SynchronizedMap[String, ContextInfo]
  val processMap = new mutable.HashMap[String, ActorRef]() with mutable.SynchronizedMap[String, ActorRef]

  val sparkUIConfigPath: String = "spark.ui.port"

  override def receive: Receive = {
    case CreateContext(contextName, jars, config) =>
      if (contextMap contains contextName) {
        sender ! ContextAlreadyExists
      } else if (jars.isEmpty) {
        sender ! ContextActor.FailedInit("jars property is not defined or is empty.")
      } else {
        //adding the default configs
        var mergedConfig = config.withFallback(defaultConfig)

        //The port for the actor system
        val port = ActorUtils.findAvailablePort(lastUsedPort)
        lastUsedPort = port

        //If not defined, setting the spark.ui port
        if (!config.hasPath(sparkUIConfigPath)) {
          mergedConfig = addSparkUiPortToConfig(mergedConfig)
        }

        val webSender = sender()
        log.info(s"Received CreateContext message : context=$contextName jars=$jars")

        val jarsFuture = jarActor ? GetJarsPathForAll(jars, contextName)
        jarsFuture map {
          case result @ ResultJarsPathForAll(pathForClasspath, pathForSpark) =>
            log.info(s"Received jars path: $result")
            val processBuilder = createProcessBuilder(contextName, port, pathForClasspath, mergedConfig)
            val command = processBuilder.toString
            log.info(s"Starting new process for context $contextName: '$command'")
            val processActor = context.actorOf(Props(classOf[ContextProcessActor], processBuilder, contextName))
            processMap += contextName -> processActor

            val host = getValueFromConfig(defaultConfig, ActorUtils.HOST_PROPERTY_NAME, "127.0.0.1")
            val actorRef = context.actorSelection(ActorUtils.getContextActorAddress(contextName, host, port))
            sendInitMessage(contextName, port, actorRef, webSender, mergedConfig, pathForSpark)
        } onFailure {
          case e: Exception =>
            log.error(s"Failed! ${ExceptionUtils.getStackTrace(e)}")
            webSender ! e
        }
      }

    case DeleteContext(contextName) =>
      log.info(s"Received DeleteContext message : context=$contextName")
      if (contextMap contains contextName) {
        for (
          contextInfo <- contextMap remove contextName;
          processRef <- processMap remove contextName
        ) {
          contextInfo.referenceActor ! ContextActor.ShutDown()
          sender ! Success

          // Terminate process
          processRef ! ContextProcessActor.Terminate()
        }
      } else {
        sender ! NoSuchContext
      }

    case ContextProcessTerminated(contextName, statusCode) =>
      log.info(s"Received ContextProcessTerminated message : context=$contextName, statusCode=$statusCode")
      contextMap remove contextName foreach {
        case contextInfo: ContextInfo =>
          log.error(s"Removing context $contextName due to corresponding process exit with status code $statusCode")
          contextInfo.referenceActor ! DeleteContext(contextName)
      }

    case GetContext(contextName) =>
      log.info(s"Received GetContext message : context=$contextName")
      if (contextMap contains contextName) {
        sender ! contextMap(contextName).referenceActor
      } else {
        sender ! NoSuchContext
      }

    case GetContextInfo(contextName) =>
      log.info(s"Received GetContext message : context=$contextName")
      if (contextMap contains contextName) {
        sender ! Context(contextName, contextMap(contextName).sparkUiPort)
      } else {
        sender ! NoSuchContext
      }

    case GetAllContextsForClient() =>
      log.info(s"Received GetAllContexts message.")
      sender ! Contexts(contextMap.values.map(contextInfo => Context(contextInfo.contextName, contextInfo.sparkUiPort)).toArray)

    case GetAllContexts() =>
      sender ! contextMap.values.map(_.referenceActor)
      log.info(s"Received GetAllContexts message.")
  }

  def sendInitMessage(contextName: String, port: Int, actorRef: ActorSelection, sender: ActorRef, config: Config, jarsForSpark: List[String]): Unit = {

    val sleepTime = getValueFromConfig(config, "appConf.init.sleep", 3000)
    val tries = config.getInt("appConf.init.tries")
    val retryTimeOut = config.getLong("appConf.init.retry-timeout") millis
    val retryInterval = config.getLong("appConf.init.retry-interval") millis
    val sparkUiPort = config.getString(sparkUIConfigPath)

    context.system.scheduler.scheduleOnce(sleepTime millis) {
      val isAwakeFuture = context.actorOf(ReTry.props(tries, retryTimeOut, retryInterval, actorRef)) ? IsAwake
      isAwakeFuture.map {
        case isAwake =>
          log.info(s"Remote context actor is awaken: $isAwake")
          val initializationFuture = actorRef ? ContextActor.Initialize(contextName, config, jarsForSpark)
          initializationFuture map {
            case success: ContextActor.Initialized =>
              log.info(s"Context '$contextName' initialized: $success")
              contextMap += contextName -> ContextInfo(contextName, sparkUiPort, actorRef)
              sender ! Context(contextName, sparkUiPort)
            case error @ ContextActor.FailedInit(reason) =>
              log.error(s"Init failed for context $contextName", reason)
              sender ! error
              processMap.remove(contextName).get ! ContextProcessActor.Terminate()
          } onFailure {
            case e: Exception =>
              log.error("FAILED to send init message!", e)
              sender ! ContextActor.FailedInit(ExceptionUtils.getStackTrace(e))
              processMap.remove(contextName).get ! ContextProcessActor.Terminate()
          }
      } onFailure {
        case e: Exception =>
          log.error("Refused to wait for remote actor, consider it as dead!", e)
          sender ! ContextActor.FailedInit(ExceptionUtils.getStackTrace(e))
      }
    }
  }

  def addSparkUiPortToConfig(config: Config): Config = {
    lastUsedPortSparkUi = ActorUtils.findAvailablePort(lastUsedPortSparkUi)
    val map = new util.HashMap[String, String]()
    map.put(sparkUIConfigPath, lastUsedPortSparkUi.toString)
    val newConf = ConfigFactory.parseMap(map)
    newConf.withFallback(config)
  }

  def createProcessBuilder(contextName: String, port: Int, jarsForClasspath: String, config: Config): ProcessBuilder = {
    val scriptPath = ContextManagerActor.getClass.getClassLoader.getResource("context_start.sh").getPath
    val xmxMemory = getValueFromConfig(config, "driver.xmxMemory", "1g")

    // Create context process directory
    val processDirName = new java.io.File(defaultConfig.getString("context.contexts-base-dir")).toString + s"/$contextName"

    Process(scriptPath, Seq(jarsForClasspath, contextName, port.toString, xmxMemory, processDirName))
  }
}

