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

import org.apache.kafka.connect.errors.ConnectException
import com.datastax.driver.core._
import org.joda.time.{DateTimeZone, DateTime}

object Types {

  /** A Kafka topic name from config. */
  type TopicName = String

  /** A Cassandra keyspace name from config. */
  type KeyspaceName = String

  /** A Cassandra table name from config. */
  type TableName = String

  /** The formatted key to get topic to QueryNamespace mappings from config. */
  type Key = String

  type Cql = String
  type Token = String
  type Clause = String
  type OrderClause = Clause
  type PredicatesClause = Clause
  type LimitClause = Clause
  type SelectedColumns = Clause
  type FilteringClause = Clause
  type ColumnName = String
}

/** INTERNAL API */
private[cassandra] object Syntax {

  final val Insert = "insert"
  final val Select = "select"
  final val From = "from"
  final val Where = "where"
  final val OrderByClause = "order by"
  final val ClusteringOrderBy = "clustering " + OrderBy
  final val Asc = "asc"
  final val Desc = "desc"
  final val And = "and"
  final val Limit = "limit"
  final val Filtering = "allow filtering"
  final val PreviousTime = "previousTime()"
  final val CurrentTime = "currentTime()"
  final val RpcAddressColumn = "rpc_address"
  final val ListenAddressColumn = "peer"
  final val SystemLocal = "system.local"
  final val SystemPeers = "system.peers"
  final val EmptyClause = ""
  def quote(name: String): String = "\"" + name + "\""


  import Types._,InternalConfig.Schema

  trait Query {
    def cql: Cql
  }

  /** The provided user's insert statement as prepared statement query.
    * Used prior to `cql` if conditions amount to increased efficiency.
    * Not simple to infer from a collection of kafka `SinkRecord`s
    * holding just the topic, so a few best efforts are made.
    */
  final case class PreparedQuery private(cql: Cql) extends Query

  object PreparedQuery {
    def apply(schema: Schema): PreparedQuery = {
      val columns = schema.columnNames.mkString(",")
      val bind = schema.columnNames.map(v => "?").mkString(",")
      PreparedQuery(s"INSERT INTO ${schema.namespace}($columns) VALUES($bind)")
    }
  }

  /** Cassandra table writes for a Kafka Connect task.
    *
    * Example:
    * {{{
    *   INSERT INTO $namespace($columns) VALUES($columnValues)
    * }}}
    *
    * @param cql the provided user's insert query
    */
  final case class SinkQuery private(cql: Cql) extends Query

  object SinkQuery {

    def valid(namespace: String): Boolean = {
      namespace.length >= 3 || namespace.contains(".")
    }

    def apply(namespace: String, columnNamesVsValues: Map[ColumnName, String]): SinkQuery = {
      val query = columnNamesVsValues.view.map(e => Vector(e._1, e._2)).transpose match {
        case columnNames :: columnValues :: Nil =>
          s"INSERT INTO ${namespace}(${columnNames.mkString(",")}) VALUES(${columnValues.mkString(",")})"
      }
      SinkQuery(query)
    }
  }

  /** Cassandra table reads for a Kafka Connect task.
    *
    * Example:
    * {{{
    *   SELECT $columns FROM $keyspace.$table WHERE $filter $orderBy $limit ALLOW FILTERING
    * }}}
    *
    * @param cql the provided user's select query
    */
  final case class SourceQuery private(cql: String,
                                       primaryKeys: List[ColumnName],
                                       pollInterval: Long,
                                       utc: Boolean) extends Query with QueryParser {

    /** WIP and Temporary (until Cassandra CDC ticket):
      *
      * It is difficult to infer timeseries from a CQL query, as it depends on the data model.
      * Until CDC: we make a best effort.
      */
    def slide: SourceQuery = cql match {
      case query if hasPatternT =>
        val now = if (utc) new DateTime(DateTimeZone.UTC).getMillis else System.nanoTime
         copy(cql = cql
           .replaceAllLiterally(PreviousTime, s"${now - pollInterval}")
           .replaceAllLiterally(CurrentTime, s"$now"))
      case query if hasRange(primaryKeys) =>
        //TODO move range tokens https://tuplejump.atlassian.net/browse/DB-56
        this
      case _ =>
        this
    }
  }

  object SourceQuery {

    def valid(cql: Cql): Boolean = {
      val query = cql.toLowerCase
      query.startsWith(Syntax.Select) && query.contains(Syntax.From)
    }

    /** Returns a new `SourceQuery` if params are valid.
      *
      * @param query the user's query where `cql` might look like:
      * {{{
      *   SELECT $columns FROM $keyspace.$table WHERE $filter $orderBy $limit ALLOW FILTERING
      * }}}
      * @param schema the schema mapping
      * @param interval the poll interval
      * @param utc `true` for UTC time zone on prev/current time slides, `false` for system time
      */
    def apply(query: Cql, schema: Schema, interval: Long, utc: Boolean): Option[SourceQuery] = {
      if (valid(query)) Some(SourceQuery(query, schema.primaryKeys, interval, utc)) else None
    }
  }

 trait QueryParser {

    def cql: Cql

    def hasPatternT: Boolean = cql.contains(PreviousTime) || cql.contains(CurrentTime)

    /** Attempts to detect a very simple range query.
      *
      * {{{
      *   CREATE TABLE fu (
      *     key text,
      *     detail_key int,
      *     detail_value text,
      *     PRIMARY KEY (key, detail_key)
      *  )
      * }}}
      *
      * In this case we detect the primary key, `detail_key`, is used as a range:
      * {{{
      *  select * from fu where key = '1' and detail_key > 2 and detail_key <=4;
      * }}}
      *
      *  Other sample range query formats:
      * {{{
      *   SELECT * FROM test WHERE token(k) > token(42);
      *   SELECT * FROM test WHERE fu >= $startToken AND fu < $endToken;
      * }}}
      */
    def hasRange(primaryKeys: List[ColumnName]): Boolean =
      cql.split(Where).lastOption.exists(hasSimpleRange(_,primaryKeys))

    def hasSimpleRange(clause: Clause, primaryKeys: List[ColumnName]): Boolean =
      clause.split(" ")
        .filter(c => primaryKeys contains c)
        .groupBy(x => x)
        .mapValues(_.length)
        .exists(_._2 > 1) // is repeated
  }

  /** A single token from:
    * {{{
    *    WHERE s"$token >= $startToken
    *    AND $token < $endToken"
    * }}}
    * would be {{{ s"$token >= $startToken" }}}. An instance of a `WhereClause`
    * holds any WHERE predicates for use.
    */
  final case class WhereClause(predicates: Seq[String], values: Seq[Any])

  object WhereClause {
    val AllRows = WhereClause(Nil, Nil)
  }

  sealed trait OrderBy
  object OrderBy {
    case object Ascending extends OrderBy
    case object Descending extends OrderBy

    def apply(cql: Cql): OrderBy =
      if (cql.contains(Syntax.Asc)) Ascending
      else Descending
  }

  sealed trait Order {
    def by: OrderBy
  }

  /* CLUSTERING ORDER BY */
  final case class ClusteringOrder(by: OrderBy) extends Order

  object ClusteringOrder {
    def apply(table: TableMetadata): Option[ClusteringOrder] =
      if (table.hasClusteringOrder) Some(ClusteringOrder(OrderBy(table.cql)))
      else None
  }
}


/** INTERNAL API */
private[cassandra] trait CassandraClusterApi {
  import InternalConfig.Route
  import Syntax._

  def session: Session

  protected lazy val metadata = session.getCluster.getMetadata

  /** Returns table metadata if the keyspace and table exist in the Cassandra cluster
    * being connected to, and the coordinates have been configured.
    *
    * @throws org.apache.kafka.connect.errors.ConnectException
    *        The datastax java driver returns `null` Keyspace or `null` Table
    *        if either do not exist, so we alert the user application via
    *        ConnectException and do not proceed, to not propagate NPEs.
    */
  protected def tableFor(ns: Route): Option[TableMetadata] =
    for {
      keyspace <- Option(metadata.getKeyspace(ns.keyspace)).orElse(throw new ConnectException(
        s"Keyspace '${ns.keyspace}' does not exist."))
      table <- Option(keyspace.getTable(ns.table)).orElse(throw new ConnectException(
        s"Table '${ns.table}' in keyspace '${ns.keyspace}' does not exist."))
    } yield table

  /** Returns the FQCN of the partitioner, which will NOT be on the classpath.
    * org.apache.cassandra.dht.{Murmur3Partitioner, RandomPartitioner...}
    */
  protected def partitioner: String = {
    session.execute(s"SELECT partitioner FROM $SystemLocal").one().getString(0)
  }

  // TODO: refactor when CASSANDRA-9436
  protected def localNode(rpcAddressColumn: String, listenAddressColumn: String): ResultSet =
    session.execute(s"SELECT $rpcAddressColumn, $listenAddressColumn FROM $SystemPeers")
}
