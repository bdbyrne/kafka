/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package kafka.server

import kafka.server.QuotaType._
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.requests.{ApiError, DescribeConfigsResponse}
import org.apache.kafka.common.requests.DescribeConfigsResponse.ConfigSource
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.utils.{Sanitizer, Time}
import org.apache.kafka.server.quota.{ClientQuotaCallback, ClientQuotaType}

import scala.collection.JavaConverters._

object QuotaType  {
  case object Fetch extends QuotaType
  case object Produce extends QuotaType
  case object Request extends QuotaType
  case object LeaderReplication extends QuotaType
  case object FollowerReplication extends QuotaType
  case object AlterLogDirsReplication extends QuotaType
}
sealed trait QuotaType

object QuotaFactory extends Logging {

  object UnboundedQuota extends ReplicaQuota {
    override def isThrottled(topicPartition: TopicPartition): Boolean = false
    override def isQuotaExceeded: Boolean = false
    def record(value: Long): Unit = ()
  }

  case class QuotaManagers(fetch: ClientQuotaManager,
                           produce: ClientQuotaManager,
                           request: ClientRequestQuotaManager,
                           leader: ReplicationQuotaManager,
                           follower: ReplicationQuotaManager,
                           alterLogDirs: ReplicationQuotaManager,
                           clientQuotaCallback: Option[ClientQuotaCallback]) {

    /**
     * Describes the configurable quota properties for a (user, client ID) pair.
     */
    def describeConfig(sanitizedUser: String, sanitizedClientId: String, includeSynonyms: Boolean): DescribeConfigsResponse.Config = {
      val principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, Sanitizer.desanitize(sanitizedUser))
      val clientId = Sanitizer.desanitize(sanitizedClientId)

      val entries = List(
        (fetch, ClientQuotaType.FETCH, DynamicConfig.Client.ConsumerByteRateOverrideProp, KafkaConfig.ProducerQuotaBytesPerSecondDefaultProp),
        (produce, ClientQuotaType.PRODUCE, DynamicConfig.Client.ProducerByteRateOverrideProp, KafkaConfig.ConsumerQuotaBytesPerSecondDefaultProp),
        (request, ClientQuotaType.REQUEST, DynamicConfig.Client.RequestPercentageOverrideProp, "")
      )
      new DescribeConfigsResponse.Config(ApiError.NONE, entries.map {
        case (manager, quotaType, property, defaultProperty) =>
          val values = manager.quotaConfig(quotaType, principal, clientId)
          if (values.isEmpty)
            throw new IllegalStateException("Failed to resolve quota")
          val synonyms = if (includeSynonyms)
            values.drop(1).map {
              case (ConfigSource.DEFAULT_CONFIG, quota) =>
                val configSource = if (defaultProperty.isEmpty)
                  ConfigSource.DEFAULT_CONFIG
                else
                  ConfigSource.STATIC_BROKER_CONFIG
                new DescribeConfigsResponse.ConfigSynonym(defaultProperty, quota.toString, configSource)
              case (configSource, quota) =>
                new DescribeConfigsResponse.ConfigSynonym(property, quota.toString, configSource)
            }
            else List.empty
          values.head match { case (configSource, quota) =>
            new DescribeConfigsResponse.ConfigEntry(property, quota.toString, configSource, false, false, synonyms.asJava)
          }
      }.asJava)
    }

    def shutdown(): Unit = {
      fetch.shutdown
      produce.shutdown
      request.shutdown
      clientQuotaCallback.foreach(_.close())
    }
  }

  def instantiate(cfg: KafkaConfig, metrics: Metrics, time: Time, threadNamePrefix: String): QuotaManagers = {

    val clientQuotaCallback = Option(cfg.getConfiguredInstance(KafkaConfig.ClientQuotaCallbackClassProp,
      classOf[ClientQuotaCallback]))
    QuotaManagers(
      new ClientQuotaManager(clientFetchConfig(cfg), metrics, Fetch, time, threadNamePrefix, clientQuotaCallback),
      new ClientQuotaManager(clientProduceConfig(cfg), metrics, Produce, time, threadNamePrefix, clientQuotaCallback),
      new ClientRequestQuotaManager(clientRequestConfig(cfg), metrics, time, threadNamePrefix, clientQuotaCallback),
      new ReplicationQuotaManager(replicationConfig(cfg), metrics, LeaderReplication, time),
      new ReplicationQuotaManager(replicationConfig(cfg), metrics, FollowerReplication, time),
      new ReplicationQuotaManager(alterLogDirsReplicationConfig(cfg), metrics, AlterLogDirsReplication, time),
      clientQuotaCallback
    )
  }

  def clientProduceConfig(cfg: KafkaConfig): ClientQuotaManagerConfig = {
    if (cfg.producerQuotaBytesPerSecondDefault != Long.MaxValue)
      warn(s"${KafkaConfig.ProducerQuotaBytesPerSecondDefaultProp} has been deprecated in 0.11.0.0 and will be removed in a future release. Use dynamic quota defaults instead.")
    ClientQuotaManagerConfig(
      quotaBytesPerSecondDefault = cfg.producerQuotaBytesPerSecondDefault,
      numQuotaSamples = cfg.numQuotaSamples,
      quotaWindowSizeSeconds = cfg.quotaWindowSizeSeconds
    )
  }

  def clientFetchConfig(cfg: KafkaConfig): ClientQuotaManagerConfig = {
    if (cfg.consumerQuotaBytesPerSecondDefault != Long.MaxValue)
      warn(s"${KafkaConfig.ConsumerQuotaBytesPerSecondDefaultProp} has been deprecated in 0.11.0.0 and will be removed in a future release. Use dynamic quota defaults instead.")
    ClientQuotaManagerConfig(
      quotaBytesPerSecondDefault = cfg.consumerQuotaBytesPerSecondDefault,
      numQuotaSamples = cfg.numQuotaSamples,
      quotaWindowSizeSeconds = cfg.quotaWindowSizeSeconds
    )
  }

  def clientRequestConfig(cfg: KafkaConfig): ClientQuotaManagerConfig = {
    ClientQuotaManagerConfig(
      numQuotaSamples = cfg.numQuotaSamples,
      quotaWindowSizeSeconds = cfg.quotaWindowSizeSeconds
    )
  }

  def replicationConfig(cfg: KafkaConfig): ReplicationQuotaManagerConfig = {
    ReplicationQuotaManagerConfig(
      numQuotaSamples = cfg.numReplicationQuotaSamples,
      quotaWindowSizeSeconds = cfg.replicationQuotaWindowSizeSeconds
    )
  }

  def alterLogDirsReplicationConfig(cfg: KafkaConfig): ReplicationQuotaManagerConfig = {
    ReplicationQuotaManagerConfig(
      numQuotaSamples = cfg.numAlterLogDirsReplicationQuotaSamples,
      quotaWindowSizeSeconds = cfg.alterLogDirsReplicationQuotaWindowSizeSeconds
    )
  }
}
