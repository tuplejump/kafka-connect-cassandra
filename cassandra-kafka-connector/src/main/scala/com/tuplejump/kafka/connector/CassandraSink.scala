/*
 * Licensed to Tuplejump Software Pvt. Ltd. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Tuplejump Software Pvt. Ltd. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.tuplejump.kafka.connector

import java.util.{List => JList, Map => JMap}

import scala.collection.JavaConverters._
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkConnector
import CassandraConnectorConfig._

class CassandraSink extends SinkConnector {

  private var configProperties = Map.empty[String, String].asJava

  override def taskClass: Class[_ <: Task] = classOf[CassandraSinkTask]

  override def taskConfigs(maxTasks: Int): JList[JMap[String, String]] = {
    List.fill(maxTasks)(configProperties).asJava
  }

  override def stop(): Unit = {}

  override def start(props: JMap[String, String]): Unit = {
    if (isValidConfig(props)) {
      configProperties = props
    } else {
      throw new ConnectException(
        s"""Couldn't start CassandraSink due to configuration error.`topics` property cannot be empty and
            |there should be a `<topicName>_table` key whose value is `<keyspace>.<tableName>` for every topic.""".stripMargin)
    }
  }

  override def version(): String = CassandraConnectorInfo.version

  private def isValidConfig(config: JMap[String, String]): Boolean = {
    val topics: String = config.getOrDefault(SinkConnector.TOPICS_CONFIG, "")
    topics.nonEmpty && topics.split(TopicSeparator).forall {
      x =>
        val key = tableConfig(x)
        val value = Option(config.get(key))
        value.isDefined && value.get.split("\\.").length == 2
    }
  }
}

class CassandraConnectorException(msg: String) extends RuntimeException(msg)

object CassandraConnectorConfig {
  val HostConfig = "host"
  val PortConfig = "port"

  val DefaultHost = "localhost"
  val DefaultPort = "9042"

  val TopicSeparator = ","

  def tableConfig(topic: String): String = {
    if (Option(topic).isDefined && topic.trim.nonEmpty) {
      s"${topic.trim}_table"
    } else {
      throw new CassandraConnectorException("Topic name missing")
    }
  }
}

