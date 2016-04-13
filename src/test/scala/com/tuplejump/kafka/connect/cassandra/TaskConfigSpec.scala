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

import org.apache.kafka.common.config.ConfigException

class TaskConfigSpec extends AbstractSpec {
  import TaskConfig._,InternalConfig._,Syntax._

  "TaskConfig" must {
    "validate and create a new TaskConfig for a Sink with valid config" in {
      val cc = TaskConfig(sinkSchemas, sinkProperties(sinkTopicMap), sink)
      cc.sink.size should be(sinkSchemas.size)
      cc.source.isEmpty should be(true)
    }
    "validate and create a new Configuration for a Source with valid config" in {
      val query = "SELECT * FROM ks1.t1"
      val topic = topicList.head

      val schemas = for {
        route <- Route(TaskConfig.SourceRoute + topic, query)
      } yield Schema(route, Nil,Nil,Nil,Nil, query)

      val props = sourceProperties(query, topic)
      commonConfig ++ Map(TaskConfig.SourceRoute + topic -> query)
      val config = TaskConfig(schemas.toList, sourceProperties(query, topic), source)
      config.sink.isEmpty should be (true)
      config.source.size should be (1)
      for (ssc <- config.source) {
        ssc.schema.route.topic should === ("test1")
        ssc.schema.namespace should === ("ks1.t1")
        ssc.schema.route.keyspace should === ("ks1")
        ssc.schema.route.table should === ("t1")
      }
    }
    "validate and not create new Configuration source configs from invalid configuration" in {
      val schema = sinkSchemas.head

      an[ConfigException] should be thrownBy {
        TaskConfig(Nil, sourceProperties(schema.route.topic, "SELECT * FROM ks1.t1"), source)
      }
    }
    "validate and not create new Configuration sink configs from invalid configuration" in {
      val sinks = Map("a" -> "b.","a"->"b","a"->".b","a"->".","a"->"ab","a"->"")
      val props = sinkProperties(sinks)
      an[ConfigException] should be thrownBy {
        TaskConfig(Nil, props, sink)
      }
    }
    "validate and create new Topics for multiple valid entries" in {
      val config = TaskConfig(sinkSchemas, sinkProperties(sinkTopicMap), sink)
      config.sink.size should be (2)
      config.source.isEmpty should be (true)
      val consistency = DefaultSourceConsistency
      sinkSchemas.forall{ schema =>
        val sc = SinkConfig(schema, PreparedQuery(schema),
            WriteOptions(DefaultSourceConsistency, DefaultFieldMapping))
        config.sink.contains(sc)
      } should be (true)
    }
    "validate and create new SinkConfigs from valid configurations" in {
      TaskConfig(sinkSchemas, sinkProperties(sinkTopicMap), sink)
        .sink.size should be (sinkSchemas.size)
    }
    "validate and not create sink config from invalid configurations" in {
      an[ConfigException] should be thrownBy {
        TaskConfig(Nil, sinkProperties(Map("a"->"b.","a"->"b","a"->".b","a"->".","a"->"ab","a"->"")), sink)
      }
    }
    "validate and not create source config with an empty or invalid query" in {
      an[ConfigException] should be thrownBy {
        TaskConfig(Nil, sourceProperties(query = "", "topic"), source)
      }
      an[ConfigException] should be thrownBy {
        TaskConfig(Nil, sourceProperties(query = "/", "topic"), source)
      }
    }
    "validate and not create source config with an empty or invalid topic" in {
      an[ConfigException] should be thrownBy {
        TaskConfig(Nil, sourceProperties(query = "SELECT * FROM test.playlists", ""), source)
      }
    }
    "not have configurations for source if set for just sink" in {
      an[ConfigException] should be thrownBy {
        TaskConfig(Nil, sourceProperties(query = "SELECT * FROM test.playlists", "topic"), source)
      }
    }
    "not have configurations for sink if set for just source" in {
      an[ConfigException] should be thrownBy {
        TaskConfig(Nil, sinkProperties(Map("a"->"k.t","a.b"->"k.t")), sink)
      }
    }
    "validate and create new Configuration for a source query" in {
      val s = sinkSchemas.head
      val query = "SELECT * FROM test.playlists"
      an[ConfigException] should be thrownBy {
        TaskConfig(List(s), sourceProperties(query, topicList.head), source)
      }
    }
  }
}
