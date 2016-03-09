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

import java.util.{Date => JDate, Map => JMap}

import scala.collection.JavaConverters._
import com.datastax.driver.core.DataType.{Name => CQLType}
import com.datastax.driver.core.{ColumnDefinitions, Row}
import org.apache.kafka.connect.data.Schema.Type
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct, Timestamp}
import org.apache.kafka.connect.sink.SinkRecord

private[cassandra] object DataConverter {
  import CassandraConnectorConfig._

  //TODO use keySchema, partition and kafkaOffset
  def sinkRecordToQuery(sinkRecord: SinkRecord, props: JMap[String, String]): String = {
    val valueSchema = sinkRecord.valueSchema()
    val columnNames = valueSchema.fields().asScala.map(_.name()).distinct

    val keyName = tableConfig(sinkRecord.topic())
    val tableName = props.get(keyName)
    val columnValueString = valueSchema.`type`() match {
      case Type.STRUCT =>
        val tpe: Struct = sinkRecord.value().asInstanceOf[Struct]
        columnNames.map { col =>
          val colValue = tpe.get(col).toString
          val colSchema = valueSchema.field(col).schema()
          //TODO ensure all types are supported
          colSchema match {
            case x if x.`type`() == Schema.STRING_SCHEMA.`type`() =>
              s"'$colValue'"
            case x if x.name() == Timestamp.LOGICAL_NAME =>
              val time = Timestamp.fromLogical(x, tpe.get(col).asInstanceOf[JDate])
              s"$time"
            case y =>
              colValue
          }
        }.mkString(",")
      case other =>
        throw new IllegalArgumentException(s"Not yet supported type $other for 'valueSchema.`type`'.")
    }
    s"INSERT INTO $tableName(${columnNames.mkString(",")}) VALUES($columnValueString)"
  }

  def rowToStruct(schema: Schema, row: Row): Struct = {
    val struct: Struct = new Struct(schema)

    schema.fields().asScala.map {
      colDef =>
        val colName: String = colDef.name
        struct.put(colName, row.getObject(colName))
    }
    struct
  }

  def columnDefToSchema(colDefs: ColumnDefinitions): Schema = {
    val builder = SchemaBuilder.struct
    colDefs.asList().asScala.map {
      colDef =>
        val colSchema = colDef.getType.getName match {
          case CQLType.ASCII | CQLType.VARCHAR | CQLType.TEXT => Schema.STRING_SCHEMA
          case CQLType.BIGINT | CQLType.COUNTER => Schema.INT64_SCHEMA
          case CQLType.BOOLEAN => Schema.BOOLEAN_SCHEMA
          case CQLType.DECIMAL | CQLType.DOUBLE => Schema.FLOAT64_SCHEMA
          case CQLType.FLOAT => Schema.FLOAT32_SCHEMA
          case CQLType.INT => Schema.INT32_SCHEMA
          case CQLType.TIMESTAMP => Timestamp.SCHEMA
          case CQLType.VARINT => Schema.INT64_SCHEMA
          case other => throw new CassandraConnectorException(s"querying for type $other is not supported")
        }
        builder.field(colDef.getName, colSchema)
    }
    builder.build()
  }
}
