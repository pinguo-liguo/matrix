package org.apache.spark.applications.clickstream.operator

import org.apache.spark.framework.operator.{Event, AbstractOperator, OperatorConfig}
import org.apache.spark.framework.output.AbstractEventOutput
import org.apache.spark.streaming.dstream.DStream

import scala.xml._

class ClickEventOperator extends AbstractOperator with OperatorConfig {
  class ClickEventOperatorConfig(
    val name: String,
    val window: Option[Long],
    val slide: Option[Long],
    val keys: Array[String],
    val outputCls: String) extends Serializable

  override def parseConfig(conf: Node) {
    val n = (conf \ "@name").text

    val props = Array("@window", "@slide") map { p =>
      val node = conf \ "property" \ p
      if (node.length == 0) {
        None
      } else {
        Some(node.text)
      }
    }

    val keys = (conf \ "keys" \ "key").map(_.text).toArray
    val output = (conf \ "output").text

    config = new ClickEventOperatorConfig(
      n,
      props(0).map(_.toLong),
      props(1).map(_.toLong),
      keys,
      output)

    outputCls = try {
      Class.forName(config.outputCls).newInstance().asInstanceOf[AbstractEventOutput]
    } catch {
      case e: Exception => throw new Exception("class " + config.outputCls + " new instance failed")
    }

    outputCls.setOutputName(config.name)
  }

  protected var config: ClickEventOperatorConfig = _
  protected var outputCls: AbstractEventOutput = _

  override def process(stream: DStream[Event]) {
    val windowedStream = windowStream(stream, (config.window, config.slide))
    val resultStream = windowedStream map { e =>
      config.keys map { k =>
        try {
          e.keyMap(k).toLong
        } catch {
          case _: Throwable => 0l
        }
      }
    }
    outputCls.output(outputCls.preprocessOutput(resultStream))
  }

}
