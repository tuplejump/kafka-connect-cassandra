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

import java.util.{List => JList, Map => JMap, ArrayList=> JArrayList}

import scala.collection.JavaConverters._
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct, Timestamp}
import org.apache.kafka.connect.errors.DataException
import com.datastax.driver.core.DataType.{Name => CQLType}
import com.datastax.driver.core.{ColumnDefinitions, Row}

/* TODO 1. Check if a task can query multiple tables
        2. Add batch max and handling (see config)
        3. Partition and offset - Map("db" -> "database_name","table" -> "table_name") */
class CassandraSourceTask extends SourceTask with TaskLifecycle {
  import CassandraSourceTask._

  private implicit var partitions: JMap[String, String] = Map.empty[String, String].asJava

  def taskClass: Class[_ <: Task] = classOf[CassandraSourceTask]

  /** Initial implementation only supports bulk load for a query. */
  override def poll: JList[SourceRecord] = {
    var result: JList[SourceRecord] = new JArrayList[SourceRecord]()
    val offset = Map.empty[String, Any].asJava //TODO

    /* This property exists by now - validation is done prior to starting a CassandraSource. */
    val source = configuration.source.head //TODO wart

    for (row <- session.execute(source.query).iterator.asScala) {
      val record = convert(row, offset, source.topic)
      result.add(record)
    }

    result
  }
}

/** INTERNAL API. */
private[kafka] object CassandraSourceTask {
  import Configuration._

  def convert(row: Row, offset: JMap[String, Any], topic: TopicName)
             (implicit partitions: JMap[String, String]): SourceRecord = {
    val schema = fieldsSchema(row.getColumnDefinitions)
    val struct = valueStruct(schema, row)
    new SourceRecord(partitions, offset, topic, schema, struct)
  }

  def fieldsSchema(columns: ColumnDefinitions): Schema = {
    val builder = SchemaBuilder.struct

    for (column <- columns.asList.asScala) builder.field(column.getName, fieldType(column))

    builder.build()
  }

  def valueStruct(schema: Schema, row: Row): Struct = {
    val struct: Struct = new Struct(schema)

    for (field <- schema.fields.asScala) {
      val colName: String = field.name
      struct.put(colName, row.getObject(colName))
    }

    struct
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
      case other => throw new DataException(s"Querying for type $other is not supported")
    }
}

