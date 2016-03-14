/*
 * Copyright 2016 Tuplejump
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tuplejump.kafka.connect.cassandra

import org.apache.kafka.connect.sink.SinkConnector
import org.scalatest.mock.MockitoSugar
import org.scalatest.{WordSpec, FlatSpec, Matchers}

trait ConfigFixture {
  import CassandraCluster._,Configuration._

  final val EmptyProperties = Map.empty[String, String]

  protected val commonConfig: Map[String, String] =
    Map(HostKey -> DefaultHosts, PortKey -> DefaultPort)

  protected def propertiesWith(topic: String, namespace: String): Map[String, String] =
    commonConfig ++ Map(topic + TopicKey -> namespace)

  protected def sinkConfig(topics:(String,String)*): Map[String, String] = {
    val topicS = (for((k,_) <- topics) yield k).mkString(Configuration.TopicSeparator)
    val topicM = Map(SinkConnector.TOPICS_CONFIG -> topicS)
    val topicConfig = (for((k,v) <- topics) yield SinkConfig.keyFor(k) -> v).toMap

    commonConfig ++ topicM ++ topicConfig
  }

  protected def sourceConfig(query: String, topic: String): Map[String,String] =
    commonConfig ++ Map(
      Configuration.QueryKey -> query,
      Configuration.TopicKey -> topic)
}

trait AbstractSpec extends WordSpec with Matchers with ConfigFixture

trait AbstractFlatSpec extends FlatSpec with Matchers with ConfigFixture with MockitoSugar
