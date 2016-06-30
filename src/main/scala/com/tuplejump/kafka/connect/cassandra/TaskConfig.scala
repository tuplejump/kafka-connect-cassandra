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
import scala.util.control.NonFatal
import scala.util.parsing.json.JSON
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.errors.DataException
import com.datastax.driver.core.{TableMetadata, ConsistencyLevel}
import InternalConfig._

/** A [[CassandraSourceTask]] will have [[TaskConfig.source]] list.
  *
  * A [[CassandraSinkTask]] will have [[TaskConfig.sink]] list.
  */
final class TaskConfig(val source: immutable.List[SourceConfig],
                       val sink: immutable.List[SinkConfig])

object TaskConfig {
  import Types._

  final val Empty = new TaskConfig(Nil, Nil)

  def apply(routes: Map[Route, Option[TableMetadata]],
            config: Map[String,String],
            taskClass: Class[_ <: Task]): TaskConfig = {

    val schemas = for {
      (route, optT) <- routes.toList
      schema        <- Schema(route, optT)
    } yield schema

    apply(schemas, config, taskClass)
  }

  def apply(schemas: List[Schema], config: Map[String,String], taskClass: Class[_ <: Task]): TaskConfig = {
    configRequire(schemas.nonEmpty, s"No schemas were created - invalid configuration.")

    taskClass match {
      case `source` =>
        val sources = for {
          schema <- schemas
          query  <- schema.route.query
          sc     <- SourceConfig(config, query, schema)
        } yield sc

        configRequire(sources.nonEmpty, s"No SourceConfigs created - invalid configuration: $config")
        new TaskConfig(sources, Nil)
      case `sink`   =>
        val sinks = for {
          schema <- schemas
        } yield SinkConfig(config, schema)

        configRequire(sinks.nonEmpty, s"No SinkConfigs created - invalid configuration: $config")
        new TaskConfig(Nil, sinks)
      case _        => TaskConfig.Empty
    }
  }

  /* **** Route config **** */
  final val TopicSeparator = ","
  final val RouteSeparator = "."

  /*  **** Source configurations ****  */
  final val SourceNamespace = "cassandra.source"

  /** SourceRoute + your.topic -> query */
  final val SourceRoute: Key = "cassandra.source.route."

  /** Frequency in ms to poll for new data in each table. */
  final val SourcePollInterval: Key = "cassandra.source.poll.interval"
  final val DefaultPollInterval: Long = 60 * 1000

  /** Size of Cassandra data to be read in a single Kafka task;
    * determines the number of partitions. */
  final val SourceSplitSize: Key = "cassandra.source.split.size"
  final val DefaultSplitSize = 64

  /** Number of CQL rows to fetch in a single round-trip to Cassandra. */
  final val SourceFetchSize: Key = "cassandra.source.fetch.size"
  final val DefaultFetchSize = 1000

  /** Consistency level for reads, defaults to LOCAL_ONE;
    * higher consistency level will disable data-locality. */
  final val SourceConsistency: Key = "cassandra.source.consistency"
  final val DefaultSourceConsistency = ConsistencyLevel.LOCAL_ONE

  final val SourceUtcTimezone: Key = "cassandra.source.timezone"
  final val DefaultUtcTimezone = true

  /* **** Sink configurations ****  */
  final val SinkNamespace = "cassandra.sink"

  /** For mapping each configured Kafka topic to the Cassandra `keyspace.table` in order
    * to create the mapping.
    *
    * To write events from a 'devices.user.events' Kafka topic's event stream to a
    * timeseries Cassandra cluster's keyspace 'devices_timeseries' and table 'device_events':
    *    Kafka devices.user.events => Cassandra devices_timeseries.user_events.
    * then the value of devices.user.events.table.sink should be devices_timeseries.user_events.
    * {{{
    *   Map(SinkRoute + "devices.user.events" -> "timeseries_keyspace.user_events",
    *       SinkRoute + "devices.sensor.events" -> "timeseries_keyspace.sensor_events")
    * }}}
    */
  final val SinkRoute: Key = "cassandra.sink.route."

  /** The consistency level for writes to Cassandra. */
  final val SinkConsistency: Key = "cassandra.sink.consistency"
  final val DefaultSinkConsistency = ConsistencyLevel.LOCAL_QUORUM

  final val FieldMapping: Key = "cassandra.sink.field.mapping"
  final val DefaultFieldMapping = Map.empty[String, String]

  /* **** Task config **** */
  final val TaskParallelismStrategy: Key = "cassandra.task.parallelism"

  sealed trait TaskStrategy {
    def key: Key
  }
  object TaskStrategy {
    /** One topic-to-keyspace.table per task. */
    case object OneToOne extends TaskStrategy {
      def key: Key = "one-to-one"
    }
    /** N-topic-to-keyspace.table per task. */
    case object OneToMany extends TaskStrategy {
      def key: Key = "one-to-many"
    }

    def apply(config: immutable.Map[String, String]): TaskStrategy =
      config.valueNonEmpty(TaskParallelismStrategy) match {
        case Some(strategy) if strategy == OneToMany.key => OneToMany
        case _ => OneToOne
      }
  }
}

/** INTERNAL API. */
private[cassandra] object InternalConfig {
  import com.datastax.driver.core.ConsistencyLevel
  import Types._

  /* valueOr functions */
  def toInt(a: String): Int = a.toInt
  def toLong(a: String): Long = a.toLong
  def toConsistency(a: String): ConsistencyLevel = ConsistencyLevel.valueOf(a)
  def toMap(a: String): Map[String, Any] = JSON.parseFull(a) collect {
    case data: Map[_, _] => data.asInstanceOf[Map[String, Any]]
  } getOrElse(throw new DataException(s"Field mapping type for '$a' is not supported."))


  /** A Cassandra `keyspace.table` to Kafka topic mapping.
    *
    * @param keyspace the keyspace name to use for a kafka connect task
    * @param table the table name to use for a kafka connect task
    */
  final case class Route private(topic: TopicName,
                                 keyspace: KeyspaceName,
                                 table: TableName,
                                 query: Option[Cql]) {

    def namespace: String = s"$keyspace.$table"

  }

  /** At this point `s` is known to not be empty but it must
    * have the expected length and format, which at a minimum is `a.b`.
    *
    * A `SourceConfig` has a `SourceQuery` such as:
    * {{{
    *   SELECT $columns FROM $keyspace.$table WHERE $filter $orderBy $limit ALLOW FILTERING
    * }}}
    *
    * A `SinkQuery` might look like this, based on keyspace.table:
    * {{{
    *   INSERT INTO $namespace($columns) VALUES($columnValues)
    * }}}
    */
  object Route {

    final val NamespaceRegex = """(?i)(?:from|into)\s+([^.]+)\.(\S+)""".r

    /** Created for sources and sinks, the one to one mapping of Kafka topic
      * to Cassandra keyspace.table.
      *
      * At this point we know all key value params are not null, not empty.
      *
      * @throws org.apache.kafka.common.config.ConfigException if `value` is invalid
      */
    def apply(topicKey: String, value: String): Option[Route] = {

      if (topicKey.contains(".route.") && value.contains(".")) {
        val msg = (topic: String) => s"'$value' for '$topic' must be valid and contain: keyspace.table"

        (topicKey.trim, value.toLowerCase) match {
          case (k,v) if k startsWith TaskConfig.SourceRoute =>
            val topic = k.replace(TaskConfig.SourceRoute, "")
            try {
              (for (a <- (NamespaceRegex findAllIn v).matchData) yield a match {
                case NamespaceRegex(x: String, y: String) =>
                  create(topic, Option(x), Option(y), Some(value))
                case _ =>
                  throw new ConfigException(msg(topic))
              }).flatten.toSeq.headOption //clean up
            } catch { case NonFatal(e) => throw new ConfigException(msg(topic)) }

          case (k,v) =>
            val topic = k.replace(TaskConfig.SinkRoute, "")
            configRequire(v.length >= 3, msg(topic))
            val split = v.split("\\.").toList
            configRequire(split.size == 2, msg(topic))
            create(topic, split.headOption, split.lastOption, None)
        }
      } else None
    }

    private def create(topic: TopicName,
                       keyspace: Option[KeyspaceName],
                       table: Option[TableName],
                       query: Option[Cql]): Option[Route] = {
      //the one place topic is validated for all cases, notify the user if invalid vs return None in for comp
     configRequire(Option(topic).forall(_.nonEmpty), "The topic must not be empty.")

      for {
        a <- keyspace.filter(_.nonEmpty)
        b <- table.filter(_.nonEmpty)
      } yield Route(topic, a, b, query)
    }
  }

  import org.apache.kafka.connect.sink.SinkRecord
  import Types._, Syntax._,TaskConfig._

  /** Note that the `cql` is the CREATE cql statement for the table metadata.
    * We can use this to parse options.
    */
  final case class Schema(route: Route,
                          partitionKey: List[ColumnName],
                          primaryKeys: List[ColumnName],
                          clusteringColumns: List[ColumnName],
                          columnNames: List[ColumnName],
                          cql: Cql) {

    def namespace: String = route.namespace
    def quotedValue: String = s"${quote(route.keyspace)}.${quote(route.table)}"

    /** Returns true if record and schema have same topic and fields. */
    def is(record: SinkRecord): Boolean = {
      val comparable = (list: List[String]) => list.map(_.toLowerCase)

      record.topic == route.topic && comparable(columnNames) == comparable(record.asColumnNames)
    }
  }

  object Schema {

    /** Returns Kafka topic to Cassandra keyspace and table metadata
      * if the keyspace and table exist in the Cassandra cluster
      * being connected to, and the coordinates have been configured.
      */
    def apply(route: Route, table: Option[TableMetadata]): Option[Schema] =
      for (t <- table) yield Schema(
        route,
        partitionKey = t.partitionKeyNames,
        primaryKeys = t.primaryKeyNames,
        clusteringColumns = t.clusteringColumnNames,
        columnNames = t.columnNames,
        cql = t.cql)
  }

  trait CassandraConfig {
    def schema: Schema
    def query: Query
    def options: ClusterQueryOptions
  }

  /** A Kafka Connect [[CassandraSource]] and [[CassandraSourceTask]] configuration.
    * INTERNAL API.
    */
  final case class SourceConfig(schema: Schema,
                                query: SourceQuery,
                                options: ReadOptions) extends CassandraConfig

  object SourceConfig {

    def apply(config: Map[String,String], cql: Cql, schema: Schema): Option[SourceConfig] =
      for {
        query <- SourceQuery(cql, schema,
              config.valueOr[Long](SourcePollInterval, toLong, DefaultPollInterval),
              DefaultUtcTimezone)
      } yield SourceConfig(schema, query, ReadOptions(config))
  }

  /** A Kafka Connect [[CassandraSink]] and [[CassandraSinkTask]] configuration.
    *
    * @param schema the kafka `topic` mapping to a cassandra keyspace and table,
    *               with the table schema
    */
  final case class SinkConfig(schema: Schema,
                              query: PreparedQuery,
                              options: WriteOptions) extends CassandraConfig

  /* TODO TTL, Timestamp, other write settings. Or, user pass in their insert query. */
  object SinkConfig {
    import Syntax._

    def apply(config: Map[String, String], schema: Schema): SinkConfig =
      SinkConfig(schema, PreparedQuery(schema), WriteOptions(config))
  }

  sealed trait ClusterQueryOptions

  /** Settings related for individual queries, can be set per keyspace.table. */
  final case class WriteOptions(consistency: ConsistencyLevel,
                                fieldMapping: Map[String, Any]) extends ClusterQueryOptions

  object WriteOptions {

    def apply(config: Map[String, String]): WriteOptions = {
      WriteOptions(
        consistency = config.valueOr[ConsistencyLevel](
          SinkConsistency, toConsistency, DefaultSourceConsistency),
        fieldMapping = config.valueOr[Map[String, Any]](
          FieldMapping, toMap, DefaultFieldMapping
        )
      )
    }
  }
  /** Settings related for individual queries, can be set per keyspace.table. */
  final case class ReadOptions(splitSize: Int,
                               fetchSize: Int,
                               consistency: ConsistencyLevel,
                               limit: Option[Long]) extends ClusterQueryOptions

  object ReadOptions {

    def apply(config: Map[String,String]): ReadOptions =
      ReadOptions(
        splitSize    = config.valueOr[Int](SourceSplitSize, toInt, DefaultSplitSize),
        fetchSize    = config.valueOr[Int](SourceFetchSize, toInt, DefaultFetchSize),
        consistency  = config.valueOr[ConsistencyLevel](
          SourceConsistency, toConsistency, DefaultSourceConsistency),
        None)
  }
}
