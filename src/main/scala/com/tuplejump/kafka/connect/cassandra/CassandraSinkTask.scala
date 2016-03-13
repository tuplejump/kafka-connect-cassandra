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

import java.util.{Collection => JCollection, Map => JMap, Date => JDate}

import org.apache.kafka.connect.connector.Task

import scala.collection.JavaConverters._
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}
import org.apache.kafka.connect.errors.{ConnectException, DataException}
import org.apache.kafka.connect.data.{Schema, Struct, Timestamp}
import org.apache.kafka.connect.data.Schema.Type._

class CassandraSinkTask extends SinkTask with TaskLifecycle {
  import CassandraSinkTask._

  def taskClass: Class[_ <: Task] = classOf[CassandraSinkTask]

  override def put(records: JCollection[SinkRecord]): Unit =
    records.asScala.foreach { record =>
      configuration.find(record.topic) match {
        case Some(topic) =>
          val query = convert(record, topic)
          session.execute(query)
        case other =>
          throw new ConnectException("Failed to get cassandra session.")
      }
    }

  /** This method is not relevant as we insert every received record in Cassandra. */
  override def flush(offsets: JMap[TopicPartition, OffsetAndMetadata]): Unit = ()

}

/** INTERNAL API. */
private[kafka] object CassandraSinkTask {
  import Configuration._

  /* TODO: Use keySchema, partition and kafkaOffset
   TODO: Add which types are currently supported in README */
  def convert(record: SinkRecord, sink: SinkConfig): Query = {
    val valueSchema = record.valueSchema
    val columnNames = valueSchema.fields.asScala.map(_.name).toSet
    val columnValues = valueSchema.`type`() match {
      case STRUCT =>
        val struct: Struct = record.value.asInstanceOf[Struct]
        columnNames.map(schema(valueSchema, struct, _)).mkString(",")
      case other =>
        throw new DataException(
          s"Unable to create insert statement with unsupported value schema type $other.")
    }
    s"INSERT INTO ${sink.namespace}(${columnNames.mkString(",")}) VALUES($columnValues)"
  }

  /* TODO support all types. */
  def schema(valueSchema: Schema, result: Struct, col: String): AnyRef =
    valueSchema.field(col).schema match {
      case x if x.`type`() == Schema.STRING_SCHEMA.`type`() =>
        s"'${result.get(col).toString}'"
      case x if x.name() == Timestamp.LOGICAL_NAME =>
        val time = Timestamp.fromLogical(x, result.get(col).asInstanceOf[JDate])
        s"$time"
      case y =>
        result.get(col)
    }
}