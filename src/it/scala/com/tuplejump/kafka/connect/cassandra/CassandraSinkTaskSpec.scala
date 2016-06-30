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
package com.tuplejump.kafka.connect.cassandra

import scala.collection.JavaConverters._
import scala.util.parsing.json.JSONObject
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.sink.{SinkRecord, SinkTaskContext}

class CassandraSinkTaskSpec extends AbstractFlatSpec {

  it should "start sink task" in {
    val topicName = "test_kv_topic"
    val tableName = "test.kv"
    val config = sinkProperties(Map(topicName -> tableName))

    val sinkTask = new CassandraSinkTask()
    val mockContext = mock[SinkTaskContext]

    sinkTask.initialize(mockContext)
    sinkTask.start(config.asJava)
    sinkTask.stop()
  }

  it should "save records in cassandra" in {
    val topicName = "test_kv_topic"
    val tableName = "test.kv"
    val config = sinkProperties(Map(topicName -> tableName))

    val sinkTask = new CassandraSinkTask()
    val mockContext = mock[SinkTaskContext]

    sinkTask.initialize(mockContext)
    sinkTask.start(config.asJava)

    val valueSchema = SchemaBuilder.struct.name("record").version(1)
      .field("key", Schema.STRING_SCHEMA)
      .field("value", Schema.INT32_SCHEMA).build
    val value1 = new Struct(valueSchema).put("key", "pqr").put("value", 15)
    val value2 = new Struct(valueSchema).put("key", "abc").put("value", 17)

    val record1 = new SinkRecord(topicName, 1, SchemaBuilder.struct.build, "key", valueSchema, value1, 0)
    val record2 = new SinkRecord(topicName, 1, SchemaBuilder.struct.build, "key", valueSchema, value2, 0)

    sinkTask.put(List(record1, record2).asJavaCollection)

    sinkTask.stop()

    val cc = CassandraCluster.local
    val session = cc.session
    val result = session.execute(s"select count(1) from $tableName").one()
    val rowCount = result.getLong(0)
    rowCount should be(2)
    cc.shutdown()
  }


  it should "save records in cassandra with custom field mapping" in {
    val topicName = "test_fieldmap_topic"
    val tableName = "test.fieldmap"
    val config = sinkProperties(Map(topicName -> tableName))

    val sinkTask = new CassandraSinkTask()
    val mockContext = mock[SinkTaskContext]

    val fieldMapping: JSONObject = JSONObject(Map(
      "key" -> "new_key",
      "value" -> "new_value",
      "nvalue" -> JSONObject(Map(
        "blah1" -> "new_nested",
        "blah2" -> JSONObject(Map(
          "blah2" -> "new_dnested"
        ))
      ))
    ))

    sinkTask.initialize(mockContext)
    sinkTask.start((config + ("cassandra.sink.field.mapping" -> fieldMapping.toString())).asJava)

    val doubleNestedSchema = SchemaBuilder.struct.name("dnestedSchema").version(1)
      .field("blah1", Schema.STRING_SCHEMA)
      .field("blah2", Schema.STRING_SCHEMA).build
    val nestedSchema = SchemaBuilder.struct.name("nestedSchema").version(1)
      .field("blah1", Schema.STRING_SCHEMA)
      .field("blah2", doubleNestedSchema).build
    val valueSchema = SchemaBuilder.struct.name("record").version(1)
      .field("key", Schema.STRING_SCHEMA)
      .field("value", Schema.INT32_SCHEMA)
      .field("nvalue", nestedSchema).build

    val dnestedValue1 = new Struct(doubleNestedSchema)
      .put("blah1", "dnes_blah1_1")
      .put("blah2", "dnes_blah2_1")
    val nestedValue1 = new Struct(nestedSchema)
      .put("blah1", "nes_blah1_1")
      .put("blah2", dnestedValue1)
    val value1 = new Struct(valueSchema)
      .put("key", "pqr")
      .put("value", 15)
      .put("nvalue", nestedValue1)

    val dnestedValue2 = new Struct(doubleNestedSchema)
      .put("blah1", "dnes_blah1_2")
      .put("blah2", "dnes_blah2_2")
    val nestedValue2 = new Struct(nestedSchema)
      .put("blah1", "nes_blah1_2")
      .put("blah2", dnestedValue2)
    val value2 = new Struct(valueSchema)
      .put("key", "abc")
      .put("value", 17)
      .put("nvalue", nestedValue2)

    val record1 = new SinkRecord(topicName, 1, SchemaBuilder.struct.build, "key", valueSchema, value1, 0)
    val record2 = new SinkRecord(topicName, 1, SchemaBuilder.struct.build, "key", valueSchema, value2, 0)

    sinkTask.put(List(record1, record2).asJavaCollection)

    sinkTask.stop()

    val cc = CassandraCluster.local
    val session = cc.session
    val result = session.execute(s"select count(1) from $tableName").one()
    val rowCount = result.getLong(0)
    rowCount should be(2)
    cc.shutdown()
  }
}

