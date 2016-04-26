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

import com.datastax.driver.core.{Row, ResultSet}

/** For iterating over a large, paged ResultSet more efficiently, asynchronously
  * pre-fetching the next `page`.
  *
  * @param results result set returned from the driver
  * @param pageSize if there are less than this rows available without blocking,
  *                   initiates fetching the next page
  */
class AsyncPagingSourceIterator(results: ResultSet,
                                pageSize: Int,
                                limit: Option[Long] = None) extends Iterator[Row] {

  private var _read = 0

  private[this] val iterator = results.iterator

  /** Returns the number of successfully read elements. */
  def read: Int = _read

  def done: Boolean = read == results.getAvailableWithoutFetching

  override def hasNext: Boolean = limit
    .map(max => _read < max && iterator.hasNext)
    .getOrElse(iterator.hasNext)

  /** Internal fetch attempt is async. */
  override def next(): Row = {
    if (hasNextWindow) results.fetchMoreResults()

    val n = iterator.next()
    _read += 1
    n
  }

  /** If `!isFullyFetched` alone does not guarantee the result set is not
    * exhausted. This verifies that it is. */
  private def hasNextWindow: Boolean =
    !results.isFullyFetched && !results.isExhausted &&
      results.getAvailableWithoutFetching < pageSize
}
