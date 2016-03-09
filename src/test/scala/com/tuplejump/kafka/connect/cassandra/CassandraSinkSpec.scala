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

import java.util.{Map => JMap}

import scala.collection.JavaConverters._
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkConnector
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

class CassandraSinkSpec extends FlatSpec with Matchers with MockitoSugar {
  import CassandraConnectorConfig._

  private val MULTIPLE_TOPICS: String = "test1,test2"

  private val sinkProperties: JMap[String, String] = Map(SinkConnector.TOPICS_CONFIG -> MULTIPLE_TOPICS,
    CassandraConnectorConfig.HostConfig -> "127.0.0.1", tableConfig("test1") -> "test.test1",
    tableConfig("test2") -> "test.test1").asJava

  it should "validate configuration" in {
    val cassandraSink = new CassandraSink()
    an[ConnectException] should be thrownBy {
      cassandraSink.start(Map.empty[String, String].asJava)
    }
    an[ConnectException] should be thrownBy {
      cassandraSink.start(Map(SinkConnector.TOPICS_CONFIG -> MULTIPLE_TOPICS).asJava)
    }
    an[ConnectException] should be thrownBy {
      cassandraSink.start(Map(SinkConnector.TOPICS_CONFIG -> "test", tableConfig("test") -> "test").asJava)
    }
  }

  it should "have taskConfigs" in {
    val cassandraSink: CassandraSink = new CassandraSink
    cassandraSink.start(sinkProperties)
    var taskConfigs = cassandraSink.taskConfigs(1)
    taskConfigs.size should be(1)
    taskConfigs.get(0).get(CassandraConnectorConfig.HostConfig) should be("127.0.0.1")

    taskConfigs = cassandraSink.taskConfigs(2)
    taskConfigs.size should be(2)
  }

}
