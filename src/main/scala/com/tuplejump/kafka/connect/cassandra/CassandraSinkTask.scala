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
package com.tuplejump.kafka.connect.cassandra

import java.util.{Collection => JCollection, Map => JMap}

import scala.collection.JavaConverters._
import com.datastax.driver.core.{Cluster, Session}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}

class CassandraSinkTask extends SinkTask {
  import CassandraConnectorConfig._

  private var _session: Option[Session] = None
  private var configProperties: JMap[String, String] = Map.empty[String, String].asJava

  //This has been exposed to be used while testing only
  private[connector] def getSession = _session

  override def stop(): Unit = {
    _session.map(_.getCluster.close())
  }

  override def put(records: JCollection[SinkRecord]): Unit = {
    _session match {
      case Some(session) =>
        records.asScala.foreach {
          r =>
            val query = DataConverter.sinkRecordToQuery(r, configProperties)
            session.execute(query)
        }
      case None =>
        throw new CassandraConnectorException("Failed to get cassandra session.")
    }
  }

  //This method is not relevant as we insert every received record in Cassandra
  override def flush(offsets: JMap[TopicPartition, OffsetAndMetadata]): Unit = {}

  override def start(props: JMap[String, String]): Unit = {
    configProperties = props
    val host = configProperties.getOrDefault(HostConfig, DefaultHost)
    val port = configProperties.getOrDefault(PortConfig, DefaultPort).toInt
    val cluster = Cluster.builder().addContactPoint(host).withPort(port)
    _session = Some(cluster.build().connect())
  }

  override def version(): String = CassandraConnectorInfo.version
}