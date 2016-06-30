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

import com.datastax.driver.core.{ DataType, TestUtil}
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.sink.SinkRecord

class SchemaSpec extends AbstractFlatSpec {

  it should "convert a struct schema with single field" in {
    val topic = "topicx"

    val sc = sinkConfig(topic, "keyspacex", "tablex", List("id"))
    sc.options.consistency should be (TaskConfig.DefaultSinkConsistency)
    sc.schema.columnNames should === (List("id"))
    sc.query.cql should be ("INSERT INTO keyspacex.tablex(id) VALUES(?)")

    val schema = SchemaBuilder.struct.name("record").version(1).field("id", Schema.INT32_SCHEMA).build
    val value = new Struct(schema).put("id", 1)
    val record = new SinkRecord(topic, 1, SchemaBuilder.struct.build, "key", schema, value, 0)

    sc.schema.route.topic should be (record.topic)
    sc.schema.route.keyspace should be ("keyspacex")
    sc.schema.route.table should be ("tablex")

    sc.schema is record should be (true)
    val query = record.as(sc.schema.namespace, Map.empty[String, Any])
    query.cql should be("INSERT INTO keyspacex.tablex(id) VALUES(1)")
  }

  it should "convert a struct schema with multiple fields" in {
    val topic = "test_kfk"
    val sc = sinkConfig(topic, "keyspacex", "tablex", List("available", "name", "age"))

    val schema = SchemaBuilder.struct.name("record").version(1)
      .field("available", Schema.BOOLEAN_SCHEMA)
      .field("name", Schema.STRING_SCHEMA)
      .field("age", Schema.INT32_SCHEMA).build
    val value = new Struct(schema).put("name", "user").put("available", false).put("age", 15)
    val record = new SinkRecord("test_kfk", 1, SchemaBuilder.struct.build, "key", schema, value, 0)

    schema.asColumnNames should be (sc.schema.columnNames)

    sc.schema.route.topic should be (record.topic)
    sc.schema is record should be (true)

    sc.query.cql should be ("INSERT INTO keyspacex.tablex(available,name,age) VALUES(?,?,?)")
    val query = record.as(sc.schema.namespace, Map.empty[String, Any])
    query.cql should be("INSERT INTO keyspacex.tablex(available,name,age) VALUES(false,'user',15)")
  }

  it should "convert cassandra column defs to a source schema" in {
    val colDef = Map(
      "id" -> DataType.cint(),
      "name" -> DataType.varchar())

    val columns = TestUtil.getColumnDef(colDef)
    val expectedSchema = SchemaBuilder.struct()
      .field("id", Schema.INT32_SCHEMA)
      .field("name", Schema.STRING_SCHEMA).build()

    columns.asSchema should be(expectedSchema)
  }

  it should "convert kafka schema and struct to cassandra columns and schema mapping" in {
    import scala.collection.JavaConverters._
    val topic = "a"
    val route = InternalConfig.Route(TaskConfig.SinkRoute + topic, "ks1.t1").get
    val schemaMap = new InternalConfig.Schema(route, Nil, Nil, Nil, List("available","name","age"), "")

    val schema = SchemaBuilder.struct.name("record").version(1)
      .field("available", Schema.BOOLEAN_SCHEMA)
      .field("name", Schema.STRING_SCHEMA)
      .field("age", Schema.INT32_SCHEMA).build
    val struct = new Struct(schema).put("name", "user").put("available", false).put("age", 15)
    val record = new SinkRecord(topic, 1, SchemaBuilder.struct.build, "key", schema, value, 0)

    schema.asColumnNames should ===(schemaMap.columnNames)
    schemaMap.columnNames should ===(schema.fields.asScala.map(_.name).toList)
    schemaMap is record should be (true)
  }
}
