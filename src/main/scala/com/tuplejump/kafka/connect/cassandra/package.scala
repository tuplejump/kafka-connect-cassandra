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

package com.tuplejump.kafka.connect

/** Common package operations. */
package object cassandra {
  import java.util.{List => JList, Map => JMap, Date => JDate}

  import scala.collection.JavaConverters._
  import com.datastax.driver.core._
  import com.datastax.driver.core.DataType.{Name => CQLType}
  import com.datastax.driver.core.Row
  import org.apache.kafka.connect.data.{Timestamp, Struct, SchemaBuilder, Schema}
  import org.apache.kafka.connect.errors.{ConnectException, DataException}
  import org.apache.kafka.connect.source.SourceRecord
  import org.apache.kafka.connect.data.Schema.Type._
  import org.apache.kafka.connect.sink.SinkRecord
  import org.apache.kafka.common.config.ConfigException
  import Types._,Syntax._

  lazy val EmptySources: JList[SourceRecord] = List.empty[SourceRecord].asJava

  lazy val EmptyJMap: JMap[String, Any] = Map.empty[String, Any].asJava

  lazy val EmptyJTaskConfigs: JList[JMap[String, String]] = List(Map.empty[String, String].asJava).asJava

  val source = classOf[CassandraSourceTask]

  val sink = classOf[CassandraSinkTask]

  implicit class ColumnDefinitionsOps(columns: ColumnDefinitions) {

    def asSchema: Schema = {
      val builder = SchemaBuilder.struct
      for (column <- columns.asList.asScala) builder.field(column.getName, fieldType(column))
      builder.build()
    }

    private def fieldType(column: ColumnDefinitions.Definition): Schema =
      column.getType.getName match {
        case CQLType.ASCII | CQLType.VARCHAR | CQLType.TEXT => Schema.STRING_SCHEMA
        case CQLType.BIGINT | CQLType.COUNTER => Schema.INT64_SCHEMA
        case CQLType.BOOLEAN => Schema.BOOLEAN_SCHEMA
        case CQLType.DECIMAL | CQLType.DOUBLE => Schema.FLOAT64_SCHEMA
        case CQLType.FLOAT => Schema.FLOAT32_SCHEMA
        case CQLType.INT => Schema.INT32_SCHEMA
        case CQLType.TIMESTAMP => Timestamp.SCHEMA
        case CQLType.VARINT => Schema.INT64_SCHEMA
        case other =>
          //TODO
          //BLOB,INET,UUID,TIMEUUID,LIST,SET,MAP,CUSTOM,UDT,TUPLE,SMALLINT,TINYINT,DATE,TIME
          throw new DataException(s"Querying for type $other is not supported")
      }
  }

  implicit class SinkRecordOps(record: SinkRecord) {

    def as(namespace: String): SinkQuery = {
      val schema = record.valueSchema
      val columnNames = schema.asColumnNames
      val columnValues = schema.`type`() match {
        case STRUCT =>
          val struct: Struct = record.value.asInstanceOf[Struct]
          columnNames.map(convert(schema, struct, _)).mkString(",")
        case other => throw new DataException(
          s"Unable to create insert statement with unsupported value schema type $other.")
      }
      SinkQuery(namespace, columnNames, columnValues)
    }

    /* TODO support all types. */
    def convert(schema: Schema, result: Struct, col: String): AnyRef =
      schema.field(col).schema match {
        case x if x.`type`() == Schema.STRING_SCHEMA.`type`() =>
          val fieldValue = result.get(col)
          if(fieldValue != null) s"'${fieldValue.toString}'" else "null"
        case x if x.name() == Timestamp.LOGICAL_NAME =>
          val time = Timestamp.fromLogical(x, result.get(col).asInstanceOf[JDate])
          s"$time"
        case y =>
          result.get(col)
      }

    def asColumnNames: List[ColumnName] =
      record.valueSchema.asColumnNames

  }

  implicit class RowOps(row: Row) {

    def as(topic: TopicName, partition: JMap[String, Any], offset: JMap[String, Any]): SourceRecord = {
      val schema = row.asSchema
      val struct = schema.asStruct(row)
      new SourceRecord(partition, offset, topic, schema, struct)
    }

    def asSchema: Schema = row.getColumnDefinitions.asSchema
  }

  implicit class SchemaOps(schema: Schema) {

    def asColumnNames: List[String] =
      schema.fields.asScala.map(_.name).toList

    def asStruct(row: Row): Struct = {
      val struct: Struct = new Struct(schema)

      for (field <- schema.fields.asScala) {
        val colName: String = field.name
        struct.put(colName, row.getObject(colName))
      }

      struct
    }
  }

  implicit class TableMetadataOps(table: TableMetadata) {
    import Types._,Syntax._

    def cql: Cql = table.asCQLQuery.toLowerCase

    def hasOrderClause(orderClause: OrderClause): Boolean =
      cql.contains(orderClause.toLowerCase)

    def clusteringColumns: List[ColumnMetadata] =
      table.getClusteringColumns.asScala.toList

    def hasClusteringOrder: Boolean =
      clusteringColumns.nonEmpty && hasOrderClause(ClusteringOrderBy)

    def partitionKeyNames: List[ColumnName] =
      table.getPartitionKey.asScala.toList.map(_.getName)

    def primaryKeyNames: List[ColumnName] =
      table.getPrimaryKey.asScala.toList.map(_.getName)

    def clusteringColumnNames: List[ColumnName] =
      clusteringColumns.map(_.getName)

    def columnNames: List[ColumnName] =
      table.getColumns.asScala.toList.map(_.getName)
  }

  implicit class ConfigOps(config: Map[String, String]) {
    def asJMap: JMap[String, String] = config.asJava

    /** Returns the value or `default` [[A]] from `config`.*/
    def valueOr[A](key: Key, func: (String) => A, default: A): A =
      valueNonEmpty(key) map(v => func(v.trim)) getOrElse default

    def valueOrThrow(key: Key, message: String): Option[String] =
      valueNonEmpty(key).orElse(throw new ConfigException(message))

    /** Returns the value from `config` or None if not exists. */
    def valueNonEmpty(key: String): Option[String] =
      config.get(key) collect { case value if value.trim.nonEmpty => value.trim }

    /** Returns a new map of only the non empty key-value pairs. */
    def filterNonEmpty: Map[String, String] =
      config.filter { case (k,v) => k.nonEmpty && v.nonEmpty }

    def common: Map[String,String] = {
      config.filterKeys(_.startsWith("cassandra.connection")) ++
        config.filterKeys(_.startsWith("cassandra.task"))
    }
  }

  implicit class JavaOps(config: JMap[String, String]) {
    /** Kafka Connect API provides stringly-typed params as a mutable map. */
    def immutable: Map[String, String] =
      Map.empty[String, String] ++ config.asScala
  }

  implicit final def configRequire(requirement: Boolean, message: => Any) {
    if (!requirement) throw new ConfigException(s"Requirement failed: $message")
  }
  implicit final def connectRequire(requirement: Boolean, message: => Any) {
    if (!requirement) throw new ConnectException(s"Requirement failed: $message")
  }

}
