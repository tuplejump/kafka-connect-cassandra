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

import java.util.{Map => JMap}

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.util.control.NonFatal
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.common.config.ConfigException
import com.datastax.driver.core.Session

/** INTERNAL API. */
private[kafka] trait ConnectorLike extends Logging {

  private val source = classOf[CassandraSourceTask]

  private val sink = classOf[CassandraSinkTask]

  protected var configuration = Configuration.Empty

  protected val version: String = CassandraConnectorInfo.version

  /** Reads in the user provided configuration and fails fast if not valid.
    *
    * @param config the user config
    */
  protected def configure(config: Map[String, String], taskClass: Class[_ <: Task]): Unit = {
    // work around for the enforced mutability in the kafka connect api
    configuration = Configuration(config)

    taskClass match {
      case `source` if configuration.source.isEmpty =>
        throw new ConfigException(s"""
            Unable to start ${getClass.getName}: `query` property cannot be empty.""")

      case `sink` if configuration.sink.isEmpty =>
        throw new ConfigException(s"""
           Unable to start ${getClass.getName}: `topics` property cannot be empty
           and there should be a `<topicName>_table` key whose value is
           `<keyspace>.<tableName>` for every topic.""")
      case _ =>
    }
  }
}

/** INTERNAL API. */
private[kafka] trait TaskLifecycle extends Task with ConnectorLike {

  private var cluster: Option[CassandraCluster] = None

  protected var _session: Option[Session] = None

  private[cassandra] def session: Session = _session.getOrElse(throw new IllegalStateException(
    "Sink has not been started yet or is not configured properly to connect to a cluster."))

  def taskClass: Class[_ <: Task]

  override def start(props: JMap[String, String]): Unit = {
    configure(immutable.Map.empty[String, String] ++ props.asScala, taskClass)

    val _cluster = CassandraCluster(configuration.config)//TODO clusterconf
    try {
      _session = Some(_cluster.connect)
      cluster = Some(_cluster)
    } catch {
      case NonFatal(e) =>
        logger.info("Unable to start CassandraSinkConnector, shutting down.", e)
        _cluster.shutdown()
    }
  }

  override def stop(): Unit =
    cluster foreach (_.shutdown())

}
