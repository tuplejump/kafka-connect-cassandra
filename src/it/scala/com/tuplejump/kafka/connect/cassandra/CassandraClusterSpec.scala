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

class CassandraClusterSpec extends AbstractSpec {
  import Configuration._

  private val inserts = 1000

  "CassandraCluster" must {
    "configure a cluster with default settings" in {
      val cc = CassandraCluster.local
      val cluster = cc.cluster
      cc.cluster.getClusterName.nonEmpty should be(true)
      cluster.isClosed should be(false)
      cc.shutdown()
      cluster.isClosed should be(true)
    }
    "configure a cluster with valid values and handle invalid host failover" in {
      val config = Map(CassandraCluster.HostKey -> "127.0.0.1,127.0.0.2")
      val cc = CassandraCluster(config)
      val cluster = cc.cluster
      cc.hosts.size should be(2)
      cc.hosts.head.getHostAddress should be(CassandraCluster.DefaultHosts)
      cc.shutdown()
      cluster.isClosed should be(true)
    }
    "handle whitespace in config" in {
      val cluster = CassandraCluster(Map(CassandraCluster.HostKey -> " 10.2.2.1, 127.0.0.1 "))
      cluster.hosts.size should be(2)
      cluster.shutdown()
      cluster.cluster.isClosed should be(true)
    }
    "create, use and destroy a session" in {

      val namespace: QueryNamespace = "githubstats.monthly_commits"
      val users = Map("helena" -> 1000, "shiti" -> 1000, "velvia" -> 800, "milliondreams" -> 2000)
      val data = for {
        (user, count) <- users
        commits <- 0 to count
        month <- 1 to 12
      } yield s"INSERT INTO $namespace (user,commits,month,year) values ('$user',$commits,$month,2016)"

      val cluster = CassandraCluster.local

      try {
        val session = cluster.connect
        data foreach (session.execute)

        users foreach { case (usr, commits) =>
          val results = session.execute(
            s"SELECT user,COMMITS,month FROM $namespace where user = '$usr'"
          ).all().asScala
          results.size should be(12)
          users.get(usr).foreach(c => results.head.getInt(1) should be(c))
        }
      } finally {
        cluster.shutdown()
        cluster.isClosed should be(true)
      }
    }
  }
}

