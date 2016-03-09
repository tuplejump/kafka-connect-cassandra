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

object CassandraConnectorConfig {
  val HostConfig = "host"
  val PortConfig = "port"

  val DefaultHost = "localhost"
  val DefaultPort = "9042"

  val TopicSeparator = ","

  val Query = "query"

  private[cassandra] def tableConfig(topic: String): String = {
    if (Option(topic).isDefined && topic.trim.nonEmpty) {
      s"${topic.trim}_table"
    } else {
      throw new CassandraConnectorException("Topic name missing")
    }
  }

  private[cassandra] def isValidValue(props: Map[String, String],
                                      name: String,
                                      constraint: (String) => Boolean): Boolean = {
    props.get(name) match {
      case Some(value) if value.trim.nonEmpty => constraint(value.trim)
      case _ => false
    }
  }
}

class CassandraConnectorException(msg: String) extends RuntimeException(msg)
