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

import org.apache.kafka.connect.connector.{ConnectorContext, Task}
import org.apache.kafka.connect.sink.SinkConnector

/** Cassandra [[SinkConnector]] data flow: a Kafka source
  * to a Cassandra sink.
  */
class CassandraSink extends SinkConnector with CassandraConnector {

  override val taskClass: Class[_ <: Task] = classOf[CassandraSinkTask]

  /** Roadmap: waiting for CDC so we don't have to use triggers. */
  override def initialize(ctx: ConnectorContext): Unit = ()

}
