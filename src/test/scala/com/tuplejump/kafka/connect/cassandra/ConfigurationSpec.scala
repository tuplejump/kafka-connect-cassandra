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

class ConfigurationSpec extends AbstractSpec {
  import Configuration._

  "Configuration" must {
    "validate and create a new Configuration from a valid topic and keyspace.table name" in {
      val t1 = "topic1"
      val n1 = "ks1.t1"
      val topic = SinkConfig(t1, n1, None)
      topic.topic should be (t1)
      topic.namespace should be (n1)
    }
    "validate and not create new Configuration from invalid configuration" in {
      Set(("a", "b."), ("a", "b"), ("a", ".b"), ("a", "."), ("a", "ab"), ("a", "")) foreach {
        case (k,v) => SinkConfig("topic")(sinkConfig((k,v))).isEmpty should be (true)
      }
    }
    "validate and create new Configuration for multiple valid entries" in {
      val t1 = "topic1"
      val n1 = "ks1.t1"
      val t2 = "topic2"
      val n2 = "ks1.t2"

      val topics = Configuration(sinkConfig((t1, n1),(t2, n2))).sink
      topics.size should be (2)
      topics.contains(SinkConfig(t1, n1, None)) should be (true)
      topics.contains(SinkConfig(t2, n2, None)) should be (true)
    }
    "validate and create new SinkConfigs from valid configurations" in {
      Configuration(
        sinkConfig(("a", "k.t"), ("a.b", "k.t")))
        .sink.size should be (2)
    }
    "validate and not create sink config from invalid configurations" in {
      Configuration(
        sinkConfig(("a", "b."), ("a", "b"), ("a", ".b"), ("a", "."), ("a", "ab"), ("a", "")))
        .sink.isEmpty should be (true)
    }
    "validate and not create source config from invalid configurations" in {
      Configuration(
        sourceConfig(query = "", "topic"))
        .source.isEmpty should be (true)

      Configuration(
        sourceConfig(query = "SELECT * FROM test.playlists", ""))
        .source.isEmpty should be (true)
    }
    "not have configurations for source if set for just sink" in {
      Configuration(
        sourceConfig(query = "SELECT * FROM test.playlists", "topic"))
        .sink.isEmpty should be (true)
    }
    "not have configurations for sink if set for just source" in {
      Configuration(
        sinkConfig(("a", "k.t"), ("a.b", "k.t")))
        .source.isEmpty should be (true)
    }
    "validate and create new Topics for multiple valid entries" in {
      val t1 = "topic1"
      val n1 = "ks1.t1"
      val t2 = "topic2"
      val n2 = "ks1.t2"

      val config = Configuration(sinkConfig((t1, n1),(t2, n2)))
      config.sink.size should be (2)
      config.sink.contains(SinkConfig(t1, n1, None)) should be (true)
      config.sink.contains(SinkConfig(t2, n2, None)) should be (true)
    }
    "validate and create new Configuration for a source query" in {
      val query = "SELECT * FROM test.playlists"
      val topic = "query.topic"
      val config = Configuration(sourceConfig(query, topic))
      config.source.head.topic should be (topic)
      config.source.head.query should be (query)
    }
    "extract the sink config to topics to to keyspace.table mappings" in {
      val topics = "a,b,c"
      val mapping = topics.split(TopicSeparator).map{t => (t,"k.t")}.toSeq
      implicit val config = sinkConfig(mapping:_*)
      Configuration.topicMap(topics).size should be (3)
    }
  }
}
