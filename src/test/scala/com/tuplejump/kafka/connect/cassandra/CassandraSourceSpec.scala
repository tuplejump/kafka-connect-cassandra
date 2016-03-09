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

import scala.collection.JavaConverters._
import org.apache.kafka.connect.errors.ConnectException
import org.scalatest.{FlatSpec, Matchers}

class CassandraSourceSpec extends FlatSpec with Matchers {

  it should "validate configuration" in {
    val cassandraSource = new CassandraSource()
    an[ConnectException] should be thrownBy {
      cassandraSource.start(Map.empty[String, String].asJava)
    }
    an[ConnectException] should be thrownBy {
      cassandraSource.start(Map(CassandraConnectorConfig.Query -> "").asJava)
    }
  }

  it should "have taskConfigs" in {
    val query = "Select * from test.playlists"
    val props = Map(CassandraConnectorConfig.Query -> query).asJava
    val cassandraSource = new CassandraSource
    cassandraSource.start(props)

    var taskConfigs = cassandraSource.taskConfigs(1)
    taskConfigs.size should be(1)
    taskConfigs.get(0).get(CassandraConnectorConfig.Query) should be(query)

    taskConfigs = cassandraSource.taskConfigs(2)
    taskConfigs.size should be(2)
  }
}
