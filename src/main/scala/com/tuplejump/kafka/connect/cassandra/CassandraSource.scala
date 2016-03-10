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

import java.util.{List => JList, Map => JMap}

import scala.collection.JavaConverters._
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.SourceConnector

class CassandraSource extends SourceConnector with Logging{
  private var configProperties = Map.empty[String, String].asJava

  override val taskClass: Class[_ <: Task] = classOf[CassandraSourceTask]

  override def taskConfigs(maxTasks: Int): JList[JMap[String, String]] = List.fill(maxTasks)(configProperties).asJava

  override def stop(): Unit = {
    logger.warn("Kafka Connect Cassandra is shutting down.")
  }

  override def start(props: JMap[String, String]): Unit = {
    if (isValidConfig(props.asScala.toMap)) {
      configProperties = props
    } else {
      throw new ConnectException("Couldn't start CassandraSource due to configuration error.")
    }
  }

  override def version(): String = CassandraConnectorInfo.version

  private def isValidConfig(props: Map[String, String]): Boolean = {
    val hasTopic= CassandraConnectorConfig.isValidValue(props,
      CassandraConnectorConfig.Topic, { x => x.nonEmpty })
    val hasQuery = CassandraConnectorConfig.isValidValue(props,
      CassandraConnectorConfig.Query, { x => x.nonEmpty })
    hasTopic && hasQuery
  }

}
