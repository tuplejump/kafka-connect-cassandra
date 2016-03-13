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

import scala.collection.immutable
import org.apache.kafka.connect.sink.SinkConnector
import com.datastax.driver.core.ConsistencyLevel
import Configuration._

/** A [[CassandraSourceTask]] will have a [[Configuration.source]] with one
  * topic -> query mapping.
  *
  * A [[CassandraSinkTask]] will have a [[Configuration.sink]] with one or more
  * mappings of topic -> keyspace.table.
  *
  * INTERNAL API.
  *
  * TODO CassandraConnectionConfig
  */
private[kafka] final class Configuration private(val config: immutable.Map[String,String],
                                                 val source: immutable.List[SourceConfig],
                                                 val sink: immutable.List[SinkConfig]) {

  /** Returns the [[SourceConfig]] for the `topic` if exists. */
  def find(topic: TopicName): Option[SinkConfig] =
    sink.find(_.topic == topic)

}

/** INTERNAL API. */
object Configuration {
  //import ConnectProtocol._


  /** A Cassandra `keyspace.table`. */
  type QueryNamespace = String

  /** A Kafka topic name. */
  type TopicName = String

  /** A Cassandra CQL query. */
  type Query = String

  /** The formatted key to get topic to QueryNamespace mappings from config. */
  type Key = String

  final val TopicSeparator = ","

  final val TopicKey: Key = "_table"

  final val QueryKey: Key = "query"

  final val TopicNameSize: Int = 2

  val Empty = new Configuration(Map.empty, Nil, Nil)

  /** Returns a new [[com.tuplejump.kafka.connect.cassandra.Configuration]]. */
  def apply(config: immutable.Map[String, String]): Configuration = {
    implicit val c = config
    val source = List(SourceConfig(config)).flatten
    val sinks = valueOr[List[SinkConfig]](SinkConnector.TOPICS_CONFIG, topicMap, Nil)
    new Configuration(config, source, sinks)
  }

  /** Returns the value or `default` [[A]] from `config`.
    *
    * @tparam A
    */
  private[kafka] def valueOr[A](key: Key, func: (String) => A, default: A)
                               (implicit config: Map[String, String]): A =
    config.get(key) collect {
      case value if value.trim.nonEmpty => func(value.trim)
    } getOrElse default

  /** Returns the value from `config` or None if not exists. */
  private[kafka] def getOrElse(config: Map[String, String], key: String, default: String): String =
    get(config, key) getOrElse default

  /** Returns the value from `config` or None if not exists. */
  private[kafka] def get(config: Map[String, String], key: String): Option[String] =
    config.get(key) collect { case value if value.trim.nonEmpty => value.trim }

  /** At this point `s` is known to not be empty but it must
    * have the expected format, which at a minimum is `a.b`. */
  private def valid(s: String, separator: String, length: Int): Boolean =
    s.nonEmpty && s.contains(separator) && s.length >= length &&
      s.split("\\.").forall(_.nonEmpty)

  private[kafka] def topicMap(s: String)(implicit config: Map[String, String]): List[SinkConfig] =
    s.trim.split(TopicSeparator).flatMap(SinkConfig(_)).distinct.toList

  sealed trait Mapping {
    def topic: TopicName
  }

  /** A Kafka [[CassandraSource]] and [[CassandraSourceTask]] configuration.
    * INTERNAL API.
    */
  private[kafka] final case class SourceConfig(val topic: TopicName,
                                               val query: Query,
                                               val config: Option[CassandraReadConfig]) extends Mapping

  object SourceConfig {

    def apply(config: Map[String,String]): Option[SourceConfig] =
      for {
        topic <- get(config, TopicKey)
        query <- get(config, QueryKey)
      } yield SourceConfig(topic, query, None)
  }

  /** A Kafka [[CassandraSink]] and [[CassandraSinkTask]] configuration.
    * INTERNAL API.
    *
    * @param topic the kafka `topic` name
    * @param namespace the cassandra `keyspace.table`
    */
  private[kafka] final case class SinkConfig(val topic: TopicName,
                                             val namespace: QueryNamespace,
                                             val config: Option[CassandraWriteConfig]) extends Mapping

  /** INTERNAL API. */
  private[kafka] object SinkConfig {

    def apply(topic: TopicName)(implicit config: Map[String, String]): Option[SinkConfig] =
      for {
        namespace <- get(config, keyFor(topic))
        if valid(namespace, ".", TopicNameSize)
      } yield SinkConfig(topic, namespace, None)

    def keyFor(topic: String): Key =
      topic.trim + TopicKey
  }

  final case class CassandraReadConfig(splitSizeInMB: Int,
                                       fetchSizeInRows: Int,
                                       consistency: ConsistencyLevel)

  object CassandraReadConfig {

    /** Size of Cassandra data to be read in a single Kafka task;
      * determines the number of partitions, but ignored if `splitCount` is set */
    val SplitSizeInMBKey = "cassandra.input.split.size_in_mb"
    val DefaultSplitSize = "64"

    /** Number of CQL rows to fetch in a single round-trip to Cassandra. */
    val FetchSizeInRowsKey = "cassandra.input.fetch.size_in_rows"
    val DefaultFetchSizeInRows = "1000"

    /** Consistency level for reads, defaults to LOCAL_ONE;
      * higher consistency level will disable data-locality. */
    val ConsistencyLevelKey = "cassandra.input.consistency.level"
    val DefaultConsistencyLevel = ConsistencyLevel.LOCAL_ONE

    def apply(config: Map[String, String]): CassandraReadConfig =
      CassandraReadConfig(
        splitSizeInMB = getOrElse(config, SplitSizeInMBKey, DefaultSplitSize).toInt,
        fetchSizeInRows = getOrElse(config, FetchSizeInRowsKey, DefaultFetchSizeInRows).toInt,
        consistency = ConsistencyLevel.valueOf(
          getOrElse(config, ConsistencyLevelKey, DefaultConsistencyLevel.name)
        ))
  }

  /* TDDO Batch, TTL, Timestamp. */
  final case class CassandraWriteConfig(consistency: ConsistencyLevel, parallelism: Int)

  object CassandraWriteConfig {

    def apply(config: Map[String, String]): CassandraWriteConfig =
      CassandraWriteConfig(
        consistency = ConsistencyLevel.valueOf(
          getOrElse(config, ConsistencyLevelKey, DefaultConsistencyLevel.name)),
        parallelism = getOrElse(config, ParallelismLevelKey, DefaultParallelismLevel).toInt
      )

    /** The consistency level for writes to Cassandra. */
    val ConsistencyLevelKey = "cassandra.output.consistency.level"
    val DefaultConsistencyLevel = ConsistencyLevel.LOCAL_QUORUM

    /**  The maximum total size of the batch in bytes. Overridden by BatchSizeRowsParam. */
    val BatchSizeBytesKey = "cassandra.output.batch.size.bytes"
    val BatchBufferSize = 1024

    /** Maximum number of batches executed in parallel by a task */
    val ParallelismLevelKey = "cassandra.output.concurrent.writes"
    val DefaultParallelismLevel = "5"

  }
}
