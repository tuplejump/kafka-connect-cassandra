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

import com.datastax.driver.core.{DataType, TestUtil}
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.sink.SinkRecord

class ConversionSpec extends AbstractFlatSpec {
  import Configuration._

  it should "convert a struct schema with single field" in {
    val valueSchema = SchemaBuilder.struct.name("record").version(1).field("id", Schema.INT32_SCHEMA).build
    val value = new Struct(valueSchema).put("id", 1)
    val record = new SinkRecord("topicx", 1, SchemaBuilder.struct.build, "key", valueSchema, value, 0)

    val sc = SinkConfig("topicx", "keyspacex.tablex", None)
    val query = CassandraSinkTask.convert(record, sc)
    query should be("INSERT INTO keyspacex.tablex(id) VALUES(1)")
  }

  it should "convert a struct schema with multiple fields" in {
    val valueSchema = SchemaBuilder.struct.name("record").version(1)
      .field("available", Schema.BOOLEAN_SCHEMA)
      .field("name", Schema.STRING_SCHEMA)
      .field("age", Schema.INT32_SCHEMA).build

    val value = new Struct(valueSchema).put("name", "user").put("available", false).put("age", 15)
    val record = new SinkRecord("test_kfk", 1, SchemaBuilder.struct.build, "key", valueSchema, value, 0)

    val query = CassandraSinkTask.convert(record, SinkConfig("test_kfk","test.t1", None))
    query should be("INSERT INTO test.t1(available,name,age) VALUES(false,'user',15)")
  }

  it should "convert cassandra column defs to a source schema" in {
    val colDef = Map(
      "id" -> DataType.cint(),
      "name" -> DataType.varchar())

    val columns = TestUtil.getColumnDef(colDef)

    val resultSchema = CassandraSourceTask.fieldsSchema(columns)
    val expectedSchema = SchemaBuilder.struct()
      .field("id", Schema.INT32_SCHEMA)
      .field("name", Schema.STRING_SCHEMA).build()
    resultSchema should be(expectedSchema)
  }

}
