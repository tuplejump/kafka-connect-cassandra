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

import org.apache.kafka.connect.source.SourceTaskContext
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._


class CassandraSourceTaskSpec extends FlatSpec with Matchers with MockitoSugar {

  it should "start source task" in {
    val sourceTask = new CassandraSourceTask()
    val mockContext = mock[SourceTaskContext]

    sourceTask.initialize(mockContext)

    sourceTask.start(Map.empty[String, String].asJava)
    sourceTask.getSession.isDefined should be(true)
    sourceTask.stop()
  }

  it should "fetch records from cassandra" in {
    val query = "SELECT * FROM test.playlists"

    val sourceTask = new CassandraSourceTask()
    val mockContext = mock[SourceTaskContext]

    sourceTask.initialize(mockContext)
    sourceTask.start(Map("host" -> "localhost", "query" -> query).asJava)

    val result = sourceTask.poll()

    result.size() should be(4)

    sourceTask.stop()
  }

}
