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

import java.util.{List => JList, Map => JMap}
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.immutable
import scala.util.control.NonFatal
import org.apache.kafka.connect.util.ConnectorUtils
import org.apache.kafka.connect.connector.{Connector, Task}
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.errors.ConnectException
import com.datastax.driver.core.Session

/** All Cassandra source, sink and task instances delegate to this
  * trait for configuration.
  *
  * INTERNAL API.
  */
private[kafka] trait ConnectorLike extends Logging {
  import InternalConfig.Route
  import TaskConfig.TaskStrategy

  protected final val version: String = CassandraConnectorInfo.version

  /* filtered for cassandra.source.* or cassandra.sink.*. */
  protected var configT: Map[String,String] = Map.empty

  /** Initialized as the default, user can override. */
  protected var taskStrategy: TaskStrategy = TaskStrategy.OneToOne

  protected var routes: List[Route] = Nil

  /** Reads in the user provided configuration for a given source or sink.
    * Fails fast on [[org.apache.kafka.connect.connector.Connector]] `start` if not valid.
    *
    * Because user params can be a mine field on bugs farther down stream we attempt
    * to catch as many up front as we can.
    *
    * @param conf the user config
    * @throws org.apache.kafka.common.config.ConfigException if requirements not met
    */
  protected def configure(conf: immutable.Map[String, String], taskClass: Class[_ <: Task]): Unit = {
    val valid = conf.filterNonEmpty
    logger.debug(s"Configuration validation starting with ${valid.size} from ${conf.size}")

    val key = taskClass match {
      case `source` => TaskConfig.SourceNamespace
      case `sink`   => TaskConfig.SinkNamespace
      case _        => ""
    }

    /* Filters by cassandra.source.* or cassandra.sink.* */
    configT = valid.common ++ valid.filterKeys(_.startsWith(key))
    logger.debug(s"Configuring [${configT.size}] from $key.*")

    try {
      routes = configT.flatMap { case (k,v) => Route(k, v)}.toList
      configRequire(routes.nonEmpty, "At least one topic to keyspace.table should be configured.")

      val configured = configT.count(_._1.contains(".route."))
      configRequire(routes.size == configured,
        s"Expected $configured routes based on config, ${routes.size} were valid: ${routes.mkString(",")}")
    }
    catch { case e: ConfigException => throw new ConnectException(
        s"Unable to start due to configuration error, ${e.getMessage}", e)
    }

    taskStrategy = TaskStrategy(conf.common)

    logger.info(s"Configured ${routes.size} Kafka - Cassandra mappings.")
  }
}

/** A CassandraConnector is either a [[CassandraSource]] or a [[CassandraSink]].
  * INTERNAL API.
  */
private[kafka] trait CassandraConnector extends Connector with ConnectorLike {

  /* The original values passed in by a user. */
  protected var originals: JMap[String, String] = Map.empty[String,String].asJMap

  override def start(props: JMap[String, String]): Unit = {
    connectRequire(!props.isEmpty, "Configurations must not be empty")
    originals = props
    configure(props.immutable, taskClass)
  }

  /** Task parallelism strategy configurable by user. Determines the number of input tasks,
    * and then divide them up.
    */
  override def taskConfigs(maxTasks: Int): JList[JMap[String, String]] = {
    import scala.collection.JavaConverters._
    import TaskConfig.TaskStrategy._

    val (routeConfs, common) = configT partition(_._1.contains(".route."))

    taskStrategy match {
      case OneToOne if routeConfs.size < maxTasks =>
        logger.debug(s"Creating $maxTasks task configs with task parallelism strategy 'OneToOne'.")
        (for ((k,v) <- routeConfs.toList) yield (common ++ Map(k -> v)).asJMap).asJava

      case _        =>
        logger.debug(s"Creating $maxTasks task configs with task parallelism strategy 'OneToMany'.")
        val numGroups = Math.min(routeConfs.size, maxTasks)
        val tablesGrouped = ConnectorUtils.groupPartitions(routeConfs.toList.asJava, numGroups)
        (for (group <- tablesGrouped.asScala) yield
          (common ++ (for ((k,v) <- group.asScala) yield k -> v)).asJMap).asJava
    }
  }

  override def stop(): Unit = {
    logger.info(s"${getClass.getSimpleName} shutting down.")
  }
}

/** INTERNAL API. */
private[kafka] trait CassandraTask extends Task with ConnectorLike with CassandraClusterApi {

  private val runnable = new AtomicBoolean(false)

  private var _session: Option[Session] = None

  /** The parsed values based on the config map passed in on start.
    * The Kafka mutable java map is converted to a scala immutable, parsed based on
    * whether this is for a source or sink or task thereof, then validated by type.
    *
    * If a source task, this will have one or more SourceConfigs.
    * If a sink task, this will have one or more SinkConfigs.
    * Currently we hand each task a unique topic to keyspace and table mapping.
    * Roadmap: Parallism partitioning strategy configurable by user.
    */
  protected var taskConfig = TaskConfig.Empty

  protected var cluster: Option[CassandraCluster] = None

  protected def taskClass: Class[_ <: Task]

  def session: Session = _session.getOrElse(throw new ConnectException(
    getClass.getSimpleName + " has not been started yet or is not properly configured."))

  /** Restarts call stop() then start().
    *
    * @param conf the filtered config from the Connector
    */
  override def start(conf: JMap[String, String]): Unit = {
    val config = conf.immutable
    configure(config, taskClass)

    val connector = CassandraCluster(config)

    try {
      _session = Some(connector.session)
      cluster = Some(connector)

      val routeMap = routes.map(r => r -> tableFor(r)).toMap
      taskConfig = TaskConfig(routeMap, configT, taskClass)

      runnable set true
      status()
    } catch { case NonFatal(e) =>
      logger.error(s"Unable to start ${getClass.getSimpleName}, shutting down.", e)
    }
  }

  override def stop(): Unit = {
    runnable set false
    status()
    cluster foreach (_.shutdown())
  }

  private def status(): Unit = {
    val status = if (runnable.get) s"starting with ${routes.size} routes" else s"shutting down"
    logger.info(s"${getClass.getSimpleName} $status.")//task id?
  }
}
