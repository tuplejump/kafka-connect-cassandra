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

import java.util.{Collection => JCollection, Map => JMap}

import scala.util.Try
import scala.util.control.NonFatal
import scala.collection.JavaConverters._
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.errors.ConnectException
import com.datastax.driver.core.{PreparedStatement, Session}

/** A Cassandra `SinkTask` run by a Kafka `WorkerSinkTask`. */
class CassandraSinkTask extends SinkTask with CassandraTask {
  import InternalConfig._

  protected val taskClass: Class[_ <: Task] = classOf[CassandraSinkTask]

  override def start(taskConfig: JMap[String,String]): Unit = {
    super.start(taskConfig)
    /* TODO for (tp <- context.assignment.asScala) */
  }

  /** Writes records from Kafka to Cassandra asynchronously and non-blocking. */
  override def put(records: JCollection[SinkRecord]): Unit = {
    // ensure only those topic schemas configured are attempted to store in C*
    // TODO handle reconfigure
    for (sc <- taskConfig.sink) {
      val byTopic = records.asScala.filter(_.topic == sc.schema.route.topic)
      write(sc, byTopic)
    }
  }

  private def write(sc: SinkConfig, byTopic: Iterable[SinkRecord]): Unit = {
    // TODO needs ticket: if (byTopic.size > 1) boundWrite(sc, byTopic) else
      for (record <- byTopic) {
        val query = record.as(sc.schema.namespace)
        Try(session.executeAsync(query.cql)) recover { case NonFatal(e) =>
          throw new ConnectException(
            s"Error executing ${byTopic.size} records for schema '${sc.schema}'.", e)
        }
      }
  }

  // for queries that are executed multiple times, one topic per keyspace.table
  private def boundWrite(sc: SinkConfig, byTopic: Iterable[SinkRecord]): Unit = {
    val statement = prepare(session, sc)
    val futures = for (record <- byTopic) yield {
      val query = record.as(sc.schema.namespace)
      try {
        val bs = statement.bind(query.cql)
        session.executeAsync(bs)
      } catch { case NonFatal(e) =>
        throw new ConnectException(
          s"Error executing ${byTopic.size} records for schema '${sc.schema}'.", e)
      }
    }

    // completed before exiting thread.
    for (rs <- futures) rs.getUninterruptibly
  }

  private def prepare(session: Session, sc: SinkConfig): PreparedStatement =
    try session.prepare(sc.query.cql).setConsistencyLevel(sc.options.consistency) catch {
      case NonFatal(e) => throw new ConnectException(
        s"Unable to prepare statement ${sc.query.cql}: ${e.getMessage}", e)
    }

  /** This method is not relevant as we insert every received record in Cassandra. */
  override def flush(offsets: JMap[TopicPartition, OffsetAndMetadata]): Unit = ()

}
