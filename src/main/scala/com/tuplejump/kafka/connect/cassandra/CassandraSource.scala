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

import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.source.SourceConnector

/** Cassandra [[SourceConnector]] data flow: a Cassandra
  * source with a Kafka sink.
  */
class CassandraSource extends SourceConnector with CassandraConnector {

  override val taskClass: Class[_ <: Task] = classOf[CassandraSourceTask]

}
