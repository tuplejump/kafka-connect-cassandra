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

import java.util.{List => JList, Map => JMap, ArrayList=> JArrayList}

import scala.collection.JavaConverters._
import com.datastax.driver.core.{Row, Cluster, Session}
import org.apache.kafka.connect.data.{Schema, Struct}
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}
 
class CassandraSourceTask extends SourceTask {
  import CassandraConnectorConfig._

  private var _session: Option[Session] = None
  private var configProperties: JMap[String, String] = Map.empty[String, String].asJava

  //This has been exposed to be used while testing only
  private[connector] def getSession = _session

  //TODO figure out what should be sourcePartition and sourceOffset
  /*From SourceTask it should be something like -Map("db" -> "database_name","table" -> "table_name").asJava*/
  private var sourcePartition: JMap[String, String] = Map.empty[String, String].asJava

  override def stop(): Unit = {
    _session.map(_.getCluster.close())
  }

  //Initial implementation only supports bulk load for a query
  override def poll(): JList[SourceRecord] = {
    val query = configProperties.get(CassandraConnectorConfig.Query) //this property should be there. validation is done prior to starting a CassandraSource
    var result: JList[SourceRecord] = new JArrayList[SourceRecord]()
    _session match {
      case Some(session) =>
        val resultSet = session.execute(query)
        while (!resultSet.isExhausted) {
          val row: Row = resultSet.one()
          //TODO check if a task can query multiple tables
          val schema: Schema = DataConverter.columnDefToSchema(row.getColumnDefinitions)
          val valueStruct: Struct = DataConverter.rowToStruct(schema, row)
          val sinkRecord = new SourceRecord(sourcePartition, null, configProperties.get("topic"), schema, valueStruct)
          result.add(sinkRecord)
        }
        result
      case None =>
        throw new CassandraConnectorException("Failed to get cassandra session.")
    }
  }

  override def start(props: JMap[String, String]): Unit = {
    configProperties = props
    val host = configProperties.getOrDefault(HostConfig, DefaultHost)
    val port = configProperties.getOrDefault(PortConfig, DefaultPort).toInt
    val cluster = Cluster.builder().addContactPoint(host).withPort(port)
    _session = Some(cluster.build().connect())
  }

  override def version(): String = CassandraConnectorInfo.version
}
