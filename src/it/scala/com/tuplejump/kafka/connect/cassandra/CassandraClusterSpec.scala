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

class CassandraClusterSpec extends AbstractSpec {

  private val inserts = 1000

  "CassandraCluster" must {
    "configure a cluster with default settings" in {
      val cluster = CassandraCluster.local
      val session = cluster.session
      session.isClosed should be(false)
      cluster.shutdown()
    }
   "configure a cluster with valid values and handle invalid host failover" in {
      val config = Map(CassandraCluster.ConnectionHosts -> "127.0.0.1")
      val cluster = CassandraCluster(config)
      cluster.seedNodes.size should be(1)
      val session = cluster.session
      cluster.shutdown()
    }
    "handle whitespace in config" in {
      val cluster = CassandraCluster(Map(CassandraCluster.ConnectionHosts -> " 10.2.2.1, 127.0.0.1 "))
      cluster.seedNodes.size should be(2)
      val session = cluster.session
      session.isClosed should be(false)
      cluster.shutdown()
    }
    "create, use and destroy a session" in {

      val namespace = "githubstats.monthly_commits"
      val users = Map("helena" -> 1000, "shiti" -> 1000, "velvia" -> 800, "milliondreams" -> 2000)
      val columnNames = List("user","commits","year","month")
      val columns = columnNames.mkString(",")

      val statements = for {
        (user, count) <- users
        month         <- 1 to 12
        commits       <- (500 to count*month).toList
      } yield s"INSERT INTO $namespace($columns) values('$user',$commits,2016,$month)"

      val cluster = CassandraCluster.local

      try {
        val session = cluster.session
        statements foreach (q => session.execute(q))

        import scala.collection.JavaConverters._

        users foreach { case (usr, commits) =>
          val query = s"SELECT * FROM $namespace WHERE user = '$usr'"
          val results = session.execute(query).all().asScala
          results.size should be (12)//months
        }
      } finally {
        cluster.shutdown()
      }
    }
  }
}

