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
import com.tuplejump.kafka.connect.cassandra.TaskConfig.TaskStrategy

class CassandraSourceSpec extends AbstractFlatSpec {

  private def config(taskStrategy: String) = (commonConfig ++ Map(
    TaskConfig.TaskParallelismStrategy -> taskStrategy,
    TaskConfig.SourceRoute + "playlists1" -> "Select * from music.playlists1",
    TaskConfig.SourceRoute + "playlists2" -> "Select * from music2.playlists2",
    TaskConfig.SourceRoute + "playlists3" -> "Select * from music.playlists3",
    TaskConfig.SourceRoute + "playlists4" -> "Select * from music2.playlists4")).asJMap

  it should "validate and fail on invalid CassandraSource configuration" in {
    val cassandraSource = new CassandraSource()
    an[ConnectException] should be thrownBy {
      cassandraSource.start(Map.empty[String, String].asJava)
    }
    an[ConnectException] should be thrownBy {
      cassandraSource.start(Map("test" -> "").asJava)
    }
    an[ConnectException] should be thrownBy {
      cassandraSource.start(Map(TaskConfig.SourceRoute + "test" -> "").asJava)
    }
    an[ConnectException] should be thrownBy {
      cassandraSource.start(Map(TaskConfig.SourceRoute -> "query").asJava)
    }
  }

  it should "validate valid configuration of a CassandraSource on startup - one-to-one task strategy" in {
    val cassandraSource = new CassandraSource
    cassandraSource.start(config(TaskStrategy.OneToOne.key))

    var taskConfigs = cassandraSource.taskConfigs(1)
    taskConfigs.size should be(1)
    taskConfigs.asScala forall { map =>
      map.asScala.filterKeys(_.startsWith(TaskConfig.SourceRoute)).size == 4 &&
        map.asScala.filterKeys(_.startsWith("cassandra.connection")).size == 3 &&
        map.asScala.filterKeys(_.startsWith("cassandra.task.parallelism")).size == 1
    } should be (true)

    taskConfigs = cassandraSource.taskConfigs(2)
    taskConfigs.size should be(2)
    taskConfigs.asScala forall { map =>
      map.asScala.filterKeys(_.startsWith(TaskConfig.SourceRoute)).size == 2 &&
        map.asScala.filterKeys(_.startsWith("cassandra.connection")).size == 3 &&
          map.asScala.filterKeys(_.startsWith("cassandra.task.parallelism")).size == 1
    }
  }

  it should "validate valid configuration of a CassandraSource on startup - one-to-many task strategy" in {
    val cassandraSource = new CassandraSource
    cassandraSource.start(config(TaskStrategy.OneToMany.key))

    var taskConfigs = cassandraSource.taskConfigs(1)
    taskConfigs.size should be(1)
    taskConfigs.asScala forall { map =>
      map.asScala.filterKeys(_.startsWith(TaskConfig.SourceRoute)).size == 4 &&
        map.asScala.filterKeys(_.startsWith("cassandra.connection")).size == 3 &&
        map.asScala.filterKeys(_.startsWith("cassandra.task.parallelism")).size == 1
    }

    taskConfigs = cassandraSource.taskConfigs(2)
    taskConfigs.size should be(2)
    taskConfigs.asScala forall { map =>
      map.asScala.filterKeys(_.startsWith(TaskConfig.SourceRoute)).size == 2 &&
        map.asScala.filterKeys(_.startsWith("cassandra.connection")).size == 3 &&
        map.asScala.filterKeys(_.startsWith("cassandra.task.parallelism")).size == 1
    }
  }
}
