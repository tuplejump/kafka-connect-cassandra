/*
 * Licensed to Tuplejump Software Pvt. Ltd. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Tuplejump Software Pvt. Ltd. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.tuplejump.kafka.connector

import scala.collection.JavaConverters._
import com.datastax.driver.core.{DataType, TestUtil}
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

class DataConverterSpec extends FlatSpec with Matchers with MockitoSugar {

  it should "convert a struct schema with single field" in {
    val valueSchema = SchemaBuilder.struct.name("record").version(1).field("id", Schema.INT32_SCHEMA).build
    val value = new Struct(valueSchema).put("id", 1)
    val record = new SinkRecord("test", 1, null, null, valueSchema, value, 0)
    val result = DataConverter.sinkRecordToQuery(record, Map("test_table" -> "test.t1").asJava)
    result should be("INSERT INTO test.t1(id) VALUES(1)")
  }

  it should "convert a struct schema with multiple fields" in {
    val valueSchema = SchemaBuilder.struct.name("record").version(1)
      .field("available", Schema.BOOLEAN_SCHEMA)
      .field("name", Schema.STRING_SCHEMA)
      .field("age", Schema.INT32_SCHEMA).build

    val value = new Struct(valueSchema).put("name", "user").put("available", false).put("age", 15)
    val record = new SinkRecord("test_kfk", 1, null, null, valueSchema, value, 0)
    val result = DataConverter.sinkRecordToQuery(record, Map("test_kfk_table" -> "test.kfk").asJava)
    result should be("INSERT INTO test.kfk(available,name,age) VALUES(false,'user',15)")
  }

  it should "build schema" in {
    val colDef = TestUtil.getColumnDef(Map(
      "id" -> DataType.cint(),
      "name" -> DataType.varchar())
    )

    val resultSchema = DataConverter.columnDefToSchema(colDef)
    val expectedSchema = SchemaBuilder.struct()
      .field("id", Schema.INT32_SCHEMA)
      .field("name", Schema.STRING_SCHEMA).build()
    resultSchema should be(expectedSchema)
  }

}
