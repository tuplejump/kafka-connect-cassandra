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

import com.tuplejump.kafka.connect.cassandra.TaskConfig.TaskStrategy.OneToMany
import org.apache.kafka.connect.errors.ConnectException

abstract class ConnectorLikeSpec extends AbstractSpec with ConnectorLike

class EmptyConnectorLikeSpec extends ConnectorLikeSpec {
  "ConnectorLike" must {
    "fail on empty source task config" in {
      an[ConnectException] should be thrownBy {
        configure(Map.empty[String, String], classOf[CassandraSourceTask])
      }
    }
    "fail on empty sink task config" in {
      an[ConnectException] should be thrownBy {
        configure(Map.empty[String, String], classOf[CassandraSinkTask])
      }
    }
    "fail on source task config with no route mappings" in {
      an[ConnectException] should be thrownBy {
        configure(commonConfig, classOf[CassandraSourceTask])
      }
    }
    "fail on sink task config with no route mappings" in {
      an[ConnectException] should be thrownBy {
        configure(commonConfig, classOf[CassandraSinkTask])
      }
    }
  }
}

class SourceConnectorLikeSpec extends ConnectorLikeSpec {
  "A Source ConnectorLike" must {
    "validate configuration on startup" in {
      val query = "SELECT * FROM music.playlists"
      val topic = "playlists"
      configure(sourceProperties(query, topic), source)
      configT.nonEmpty should be (true)
      configT.keySet.exists(_.contains("cassandra.connection")) should be (true)
      configT.keySet.exists(_.contains("cassandra.source")) should be (true)
      configT.keySet.exists(_.contains("cassandra.sink")) should be (false)

      taskStrategy should === (TaskConfig.TaskStrategy.OneToOne)
      routes.size should be (1)
      val route = routes.head
      route.topic should ===("playlists")
      route.keyspace should be ("music")
      route.table should be ("playlists")
      route.query should be (Some(query))
    }
  }
}

class SinkConnectorLikeSpec extends AbstractSpec with ConnectorLike {

  protected def withOneToMany(config:Map[String,String]): Map[String,String] =
    commonConfig ++ config ++ Map(TaskConfig.TaskParallelismStrategy -> OneToMany.key)

  "A Sink ConnectorLike" must {
    "validate configuration on startup" in {
      val props = withOneToMany(sinkProperties(sinkTopicMap))
      configure(props, sink)
      configT.nonEmpty should be (true)
      configT.keySet.exists(_.contains("cassandra.connection")) should be (true)
      configT.keySet.exists(_.contains("cassandra.task")) should be (true)
      configT.keySet.exists(_.contains("cassandra.sink")) should be (true)
      configT.keySet.exists(_.contains("cassandra.source")) should be (false)

      routes.size should be (2)
      taskStrategy should ===(TaskConfig.TaskStrategy.OneToMany)
    }
  }
}