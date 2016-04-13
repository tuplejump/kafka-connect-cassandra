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

import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, WordSpec, FlatSpec, Matchers}

trait ConfigFixture extends {
  import InternalConfig._,Types._,TaskConfig._

  final val EmptyProperties = Map.empty[String, String]

  protected val multipleTopics: String = "test1,test2"

  protected lazy val topicList = multipleTopics.split(TaskConfig.TopicSeparator).toList

  protected lazy val sourceSchemas: List[Schema] =
    topicList.zipWithIndex.map { case (t,i) =>
      val route = Route(TaskConfig.SourceRoute + t, s"SELECT * FROM ks$i.table$i").get
      Schema(route, Nil, Nil, Nil, List("a", "b"), "")
    }

  protected lazy val sinkSchemas: List[Schema] =
    topicList.zipWithIndex.map { case (t,i) =>
      val route = Route(TaskConfig.SinkRoute + t, s"ks$i.table$i").get
      Schema(route, Nil, Nil, Nil, List("a", "b"), "")
    }

  protected lazy val sinkTopicMap = sinkSchemas.map(s => s.route.topic -> s.namespace).toMap

  protected lazy val commonConfig = Map(
    CassandraCluster.ConnectionHosts -> CassandraCluster.DefaultHosts,
    CassandraCluster.ConnectionPort -> CassandraCluster.DefaultPort.toString,
    CassandraCluster.ConnectionConsistency -> CassandraCluster.DefaultConsistency.name
  )

  protected def sinkProperties(config:Map[String,String] = sinkTopicMap): Map[String, String] =
    config.map { case (topic,ns) => TaskConfig.SinkRoute + topic -> ns} ++ commonConfig

  protected def sourceProperties(query: String, topic: String): Map[String,String] =
    commonConfig ++ Map(TaskConfig.SourceRoute + topic -> query)

  protected def sinkConfig(topic: TopicName,
                           keyspace: KeyspaceName,
                           table: TableName,
                           columnNames: List[ColumnName] = Nil): SinkConfig = {
    import com.tuplejump.kafka.connect.cassandra.Syntax.PreparedQuery

    val route = Route(TaskConfig.SinkRoute + topic, s"$keyspace.$table").get
    val schema = Schema(route, Nil, Nil, Nil, columnNames, "")
    SinkConfig(schema, PreparedQuery(schema), WriteOptions(DefaultSinkConsistency, DefaultFieldMapping))
  }
}

trait AbstractSpec extends WordSpec with Matchers with BeforeAndAfterAll with ConfigFixture

trait AbstractFlatSpec extends FlatSpec with Matchers with BeforeAndAfterAll
  with ConfigFixture with MockitoSugar
