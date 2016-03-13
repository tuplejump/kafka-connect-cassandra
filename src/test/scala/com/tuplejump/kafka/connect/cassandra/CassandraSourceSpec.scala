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

class CassandraSourceSpec extends AbstractFlatSpec {

  it should "validate configuration" in {
    val cassandraSource = new CassandraSource()
    an[ConnectException] should be thrownBy {
      cassandraSource.start(Map.empty[String, String].asJava)
    }
    an[ConnectException] should be thrownBy {
      cassandraSource.start(Map.empty[String, String].asJava)
    }
  }

  it should "have taskConfigs" in {
    val query = "SELECT * FROM test.playlists"
    val topic = "test"
    val config = sourceConfig(query, topic)

    val cassandraSource = new CassandraSource
    cassandraSource.start(config.asJava)

    var taskConfigs = cassandraSource.taskConfigs(1)
    taskConfigs.size should be(1)
    taskConfigs.get(0).get(Configuration.QueryKey) should be(query)

    taskConfigs = cassandraSource.taskConfigs(2)
    taskConfigs.size should be(2)
  }
}
