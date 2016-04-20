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

import java.util.{List => JList, Map => JMap, ArrayList => JArrayList}

import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}

/** A Cassandra `SourceTask` run by a Kafka `WorkerSourceTask`.
  * In a Cassandra source task, the query may be selected column reads
  * of pre-aggregated data vs reflect all columns in the table schema.
  */
class CassandraSourceTask extends SourceTask with CassandraTask {
  import InternalConfig._

  //until CDC, only for ranges. not using yet, coming next PR.
  private var checkpoint: Option[Any] = None

  private var partitions = new JArrayList[JMap[String, String]]()

  protected final val taskClass: Class[_ <: Task] = classOf[CassandraSourceTask]

  override def start(taskConfig: JMap[String, String]): Unit = {
    super.start(taskConfig)
    //TODO context.offsetStorageReader.offsets(partitions)
  }

  /** Returns a list of records when available by polling this SourceTask
    * for new records. From the kafka doc: "This method should block if
    * no data is currently available."
    *
    * Initial implementation only supports bulk load for a query.
    */
  override def poll: JList[SourceRecord] = {

    val records = new JArrayList[SourceRecord]()
    val offset = EmptyJMap//context.offsetStorageReader.offset(EmptyJMap) //TODO
    val partition = EmptyJMap //TODO

    for {
      sc       <- taskConfig.source
      iterator <- page(sc)
      row      <- iterator
    } {
      val record = row.as(sc.schema.route.topic, partition, offset)
      records.add(record)
      if (iterator.done) checkpoint = None //TODO
      record
    }

    records
  }

  private def page(sc: SourceConfig): Option[AsyncPagingSourceIterator] = {
    //TODO need CDC: https://github.com/tuplejump/kafka-connector/issues/9
    val query = sc.query match {
      case q if q.hasPatternT =>
        //TODO remove Thread.sleep with better option like timestamp.fromNow...etc
        Thread.sleep(sc.query.pollInterval)
        sc.query.slide
      case q =>
        // TODO typed: https://tuplejump.atlassian.net/browse/DB-56 timeuuid,timestamp...
        // by type: WHERE {columnToMove} > checkpoint.value with columnType
        sc.query
    }

    val rs = session.execute(query.cql)
    if (rs.getAvailableWithoutFetching > 0) Some(new AsyncPagingSourceIterator(rs, sc.options.fetchSize))
    else None
  }
}
