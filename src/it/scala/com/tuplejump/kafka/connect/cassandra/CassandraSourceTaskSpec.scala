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

import java.util.concurrent.TimeUnit

import org.apache.kafka.connect.source.SourceTaskContext

import scala.collection.JavaConverters._

class CassandraSourceTaskSpec extends AbstractFlatSpec {

  val query = "SELECT * FROM test.playlists"
  val topic = "test"
  val config = sourceConfig(query, topic)

  it should "start source task" in {
    val sourceTask = new CassandraSourceTask()
    val mockContext = mock[SourceTaskContext]

    sourceTask.initialize(mockContext)

    sourceTask.start(config.asJava)
    Option(sourceTask.session).isDefined should be(true)
    sourceTask.stop()
  }

  it should "fetch records from cassandra in bulk" in {
    val sourceTask = new CassandraSourceTask()
    val mockContext = mock[SourceTaskContext]

    sourceTask.initialize(mockContext)
    sourceTask.start(config.asJava)

    val result = sourceTask.poll()

    result.size() should be(4)

    sourceTask.stop()
  }

  def insertStmt(time: Long): String = {
    "INSERT INTO test.event_store(app_id,event_type,subscription_type,event_ts) " +
      s"VALUES ('website','renewal','annual',$time)"
  }

  it should "fetch only new records from cassandra" in {
    val timeBasedQuery =
      """SELECT * FROM test.event_store WHERE app_id='website' AND event_type='renewal'
        | AND event_ts >= previousTime()""".stripMargin

    val topic = "events"
    val cassandraSourceConfig = sourceConfig(timeBasedQuery, topic)

    val sourceTask = new CassandraSourceTask()
    val mockContext = mock[SourceTaskContext]

    sourceTask.initialize(mockContext)
    sourceTask.start(cassandraSourceConfig.asJava)

    val oneHrAgo = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1)
    sourceTask.session.execute(insertStmt(oneHrAgo))

    sourceTask.poll().size() should be(0)

    val oneHrLater = System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1)
    sourceTask.session.execute(insertStmt(oneHrLater))

    val result = sourceTask.poll()

    result.size() should be(1)

    sourceTask.stop()
  }

  it should "fetch records from cassandra in given pollInterval" in {
    val timeBasedQuery =
      """SELECT * FROM test.event_store WHERE app_id='website' AND event_type='renewal'
        | AND event_ts >= previousTime() AND event_ts <= currentTime()""".stripMargin

    val topic = "events"
    val cassandraSourceConfig = sourceConfig(timeBasedQuery, topic)

    val sourceTask = new CassandraSourceTask()
    val mockContext = mock[SourceTaskContext]

    sourceTask.initialize(mockContext)
    sourceTask.start(cassandraSourceConfig.asJava)

    val oneHrLater = System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1)
    sourceTask.session.execute(insertStmt(oneHrLater))

    val fewSecLater = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(2)
    sourceTask.session.execute(insertStmt(fewSecLater))

    val result = sourceTask.poll()

    result.size() should be(1)

    sourceTask.stop()
  }
}
