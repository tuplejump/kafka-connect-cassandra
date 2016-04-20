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

class RouteSpec extends AbstractSpec {

  import InternalConfig._

  private val topics = List(
    "topic1", "devices", "test", "FuTopic", "_topic", "some-topic"
  )

  "A Route" must {
    "not create routes given invalid configuration" in {
      an[ConfigException] should be thrownBy {
        Route(TaskConfig.SinkRoute + "sensors.stream", "timeseries.")
      }

      List(
        Route("", "timeseries.sensors"),
        Route(TaskConfig.SourceRoute + "sensors.stream", "select a,b,c from x"),
        Route(TaskConfig.SourceRoute + "sensors.stream", "select * from x"),
        Route(TaskConfig.SourceRoute + "sensors.stream", "select a,b,c from"),
        Route(TaskConfig.SourceRoute + "topic", "select a,b,c from x."),
        Route(TaskConfig.SourceRoute + "topic", "select a,b,c from .y"),
        Route(TaskConfig.SourceRoute + "", ""),
        Route(TaskConfig.SourceRoute + "topic", "SELECT a,b,c FROM "),
        Route(TaskConfig.SourceRoute + "topic", "SELECT a,b,c FROM"),
        Route(TaskConfig.SourceRoute + "topic", "SELECT * ")
      ).flatten.isEmpty should be (true)

      List(
        Route(TaskConfig.SinkRoute + "sensors.stream", "a"),
        Route(TaskConfig.SinkRoute + "t", "ab"),
        Route(TaskConfig.SinkRoute + "topic", "")
      ).flatten.isEmpty should be (true)
    }

    "create routes from valid source route config" in {
      (for {
        topic <- topics
        query <- List(
            s"SELECT * FROM a.$topic WHERE token(k) > token(42)",
            s"select a from timeseries.$topic")
      } yield Route(TaskConfig.SourceRoute + topic, query))
        .flatten.size should be (topics.size * 2)
    }
    "create routes from valid sink route config" in {
      (for {
        topic <- topics
        ns    <- List(s"keyspace.$topic", "a.b", "a_x.b_x")
      } yield Route(TaskConfig.SinkRoute + topic, ns))
        .flatten.size should be (topics.size * 3)
    }
  }
}
