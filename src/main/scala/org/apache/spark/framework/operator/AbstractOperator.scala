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

import org.apache.spark.framework.operator.Event
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream

abstract class AbstractOperator extends Serializable {
  def process(stream: DStream[Event])

  def windowStream[U](stream: DStream[U],
		  window: (Option[Long], Option[Long])) = {
    window match {
    case (Some(a), Some(b)) =>
      stream.window(Seconds(a), Seconds(b))
    case (Some(a), None) =>
      stream.window(Seconds(a))
    case _ =>
      stream
    }
  }

}
