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

package org.apache.spark.framework.operator

import org.apache.spark.framework.Event
import org.apache.spark.framework.output.AbstractEventOutput
import org.apache.spark.streaming.dstream.DStream

import scala.xml._


class AggregateOperator extends AbstractOperator with OperatorConfig {
  class AggregateOperatorConfig (
    val name: String,
    val window: Option[Long],
    val slide: Option[Long],
    val key: String,
    val value: String,
    val outputClsName: String) extends Serializable

  override def parseConfig(conf: Node) {
    val nam = (conf \ "@name").text

    val propNames = Array("@window", "@slide")
    val props = propNames.map(p => {
      val node = conf \ "property" \ p
      if (node.length == 0) {
        None
      } else {
        Some(node.text)
      }
    })

    val key = (conf \ "key").text
    val value = (conf \ "value").text
    val output = (conf \ "output").text

    config = new AggregateOperatorConfig(
        nam,
        props(0) map { s => s.toLong },
        props(1) map { s => s.toLong },
        key,
        value,
        output)

    outputCls = try {
      Class.forName(config.outputClsName).newInstance().asInstanceOf[AbstractEventOutput]
    } catch {
      case _: Throwable => throw new Exception("class" + config.outputClsName + " new instance failed")
    }
    outputCls.setOutputName(config.name)
  }

  protected var config: AggregateOperatorConfig = _
  protected var outputCls: AbstractEventOutput = _

  override def process(stream: DStream[Event]) {
    val windowedStream = windowStream(stream, (config.window, config.slide))
    val resultStream = windowedStream.map(r => (r.keyMap(config.key), r.keyMap(config.value))).groupByKey()

    outputCls.output(outputCls.preprocessOutput(resultStream))
  }
}
