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

import java.util.{Collection => JCollection, Map => JMap, Date}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import com.datastax.driver.core.{Cluster, Session}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.data.Schema.Type._
import org.apache.kafka.connect.data.{Timestamp, Schema, Struct}
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}
import com.tuplejump.kafka.connector.CassandraConnectorConfig._

class CassandraSinkTask extends SinkTask {
  private var maybeSession: Option[Session] = None
  private var configProperties: JMap[String, String] = Map.empty[String, String].asJava

  def getSession = maybeSession

  override def stop(): Unit = {
    maybeSession.map(_.getCluster.close())
  }

  override def put(records: JCollection[SinkRecord]): Unit = {
    maybeSession match {
      case Some(session) =>
        records.foreach {
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
    maybeSession = Some(cluster.build().connect())
  }

  override def version(): String = CassandraConnectorInfo.version
}

object DataConverter {

  //TODO use keySchema, partition and kafkaOffset
  def sinkRecordToQuery(sinkRecord: SinkRecord, props: JMap[String, String]): String = {
    val valueSchema = sinkRecord.valueSchema()
    val columnNames = valueSchema.fields().map(_.name()).distinct

    val keyName = tableConfig(sinkRecord.topic())
    val tableName = props.get(keyName)
    val columnValueString = valueSchema.`type`() match {
      case STRUCT =>
        val result: Struct = sinkRecord.value().asInstanceOf[Struct]
        columnNames.map { col =>
          val colValue = result.get(col).toString
          val colSchema = valueSchema.field(col).schema()
          //TODO ensure all types are supported
          colSchema match {
            case x if x.`type`() == Schema.STRING_SCHEMA.`type`() =>
              s"'$colValue'"
            case x if x.name() == Timestamp.LOGICAL_NAME =>
              val time = Timestamp.fromLogical(x, result.get(col).asInstanceOf[Date])
              s"$time"
            case y =>
              colValue
          }
        }.mkString(",")
    }
    s"INSERT INTO $tableName(${columnNames.mkString(",")}) VALUES($columnValueString)"
  }
}
