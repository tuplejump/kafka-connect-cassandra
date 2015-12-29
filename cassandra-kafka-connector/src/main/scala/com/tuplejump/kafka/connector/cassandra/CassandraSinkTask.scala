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
package com.tuplejump.kafka.connector.cassandra

import java.util.{Map => JMap, Collection => JCollection}

import scala.collection.JavaConversions._
import com.datastax.driver.core.{Cluster, Session}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.data.Schema.Type._
import org.apache.kafka.connect.data.{Schema, Struct}
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}

class CassandraSinkTask extends SinkTask {
  private var maybeSession: Option[Session] = None

  def getSession = maybeSession

  override def stop(): Unit = {
    maybeSession.map(_.getCluster.close())
  }

  override def put(records: JCollection[SinkRecord]): Unit = {
    val session = maybeSession.get
    records.foreach {
      r =>
        val query = DataConverter.sinkRecordToQuery(r)
        session.execute(query)
    }
  }

  //This method is not relevant as we insert every received record in Cassandra
  override def flush(offsets: JMap[TopicPartition, OffsetAndMetadata]): Unit = {}

  override def start(props: JMap[String, String]): Unit = {
    val host = props.getOrDefault("host", "localhost")
    val port = props.getOrDefault("port", "9042").toInt
    val cluster = Cluster.builder().addContactPoint(host).withPort(port)
    maybeSession = Some(cluster.build().connect())
  }

  override def version(): String = BuildInfo.version
}

object DataConverter {

  //TODO use keySchema, partition and kafkaOffset
  def sinkRecordToQuery(sinkRecord: SinkRecord): String = {
    val valueSchema = sinkRecord.valueSchema()
    val columnNames = valueSchema.fields().map(_.name())
    val columnValueString = valueSchema.`type`() match {
      case STRUCT =>
        val result: Struct = sinkRecord.value().asInstanceOf[Struct]
        columnNames.map { col =>
          val colValue = result.get(col).toString
          if (valueSchema.field(col).schema() == Schema.STRING_SCHEMA) {
            s"'$colValue'"
          } else colValue
        }.mkString(",")
    }
    s"INSERT INTO ${sinkRecord.topic()}(${columnNames.mkString(",")}) VALUES(${columnValueString})"
  }
}
