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
import com.tuplejump.kafka.connect.cassandra.TaskConfig.TaskStrategy
import org.apache.kafka.connect.errors.ConnectException

class CassandraSinkSpec extends AbstractFlatSpec {

  private def config(taskStrategy: TaskStrategy) = (commonConfig ++ Map(
    TaskConfig.TaskParallelismStrategy -> taskStrategy.key,
    TaskConfig.SinkRoute + "devices.weather" -> "timeseries.hourly_weather1",
    TaskConfig.SinkRoute + "devices.user" -> "timeseries.user_devices1",
    TaskConfig.SinkRoute + "devices.weather2" -> "timeseries.hourly_weather2",
    TaskConfig.SinkRoute + "devices.user2" -> "timeseries.user_devices2")).asJMap

  it should "fail on invalid configurations" in {
    an[ConnectException] should be thrownBy {
      new CassandraSink().start(Map.empty[String, String].asJMap)
    }
    an[ConnectException] should be thrownBy {
      new CassandraSink().start(Map(TaskConfig.SinkRoute + "test" -> "").asJMap)
    }
    an[ConnectException] should be thrownBy {
      new CassandraSink().start(Map(TaskConfig.SinkRoute + "test" -> "test").asJMap)
    }
    an[ConnectException] should be thrownBy {
      new CassandraSink().start(Map(TaskConfig.SinkRoute + "test" -> ".test").asJMap)
    }
    an[ConnectException] should be thrownBy {
      new CassandraSink().start(Map("test" -> "ks.t").asJMap)
    }
  }

  it should "have taskConfigs with valid configurations with one to one" in {
    val cassandraSink: CassandraSink = new CassandraSink
    cassandraSink.start(config(TaskStrategy.OneToOne))

    var taskConfigs = cassandraSink.taskConfigs(1)
    taskConfigs.size should be(1)
    taskConfigs.asScala forall { map =>
      map.asScala.filterKeys(_.startsWith(TaskConfig.SinkRoute)).size == 4 &&
        map.asScala.filterKeys(_.startsWith("cassandra.connection")).size == 3 &&
        map.asScala.filterKeys(_.startsWith("cassandra.task.parallelism")).size == 1
    } should be(true)

    taskConfigs = cassandraSink.taskConfigs(2)
    taskConfigs.size should be(2)
    taskConfigs.asScala forall { map =>
      map.asScala.filterKeys(_.startsWith(TaskConfig.SinkRoute)).size == 2 &&
        map.asScala.filterKeys(_.startsWith("cassandra.connection")).size == 3 &&
        map.asScala.filterKeys(_.startsWith("cassandra.task.parallelism")).size == 1
    }
  }

  it should "validate valid configuration of a CassandraSink on startup with one to many" in {
    val cassandraSink = new CassandraSink
    cassandraSink.start(config(TaskStrategy.OneToMany))
    var taskConfigs = cassandraSink.taskConfigs(1)
    taskConfigs.size should be(1)
    taskConfigs.asScala forall { map =>
      map.asScala.filterKeys(_.startsWith(TaskConfig.SinkRoute)).size == 4 &&
        map.asScala.filterKeys(_.startsWith("cassandra.connection")).size == 3 &&
        map.asScala.filterKeys(_.startsWith("cassandra.task.parallelism")).size == 1
    } should be(true)

    taskConfigs = cassandraSink.taskConfigs(2)
    taskConfigs.size should be(2)
    taskConfigs.asScala forall { map =>
      map.asScala.filterKeys(_.startsWith(TaskConfig.SinkRoute)).size == 2 &&
        map.asScala.filterKeys(_.startsWith("cassandra.connection")).size == 3 &&
        map.asScala.filterKeys(_.startsWith("cassandra.task.parallelism")).size == 1
    }
  }
}
