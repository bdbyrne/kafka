/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.producer.internals.TopicInfo;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class ProducerMetadata extends Metadata {
    static final long TOPIC_EXPIRY_MS = 5 * 60 * 1000;
    static final int TOPIC_REFRESH_TARGET_SIZE = 25;

    private final Map<String, TopicInfo> topics = new HashMap<>();
    private final Logger log;
    private final Time time;
    private final TopicInfo refreshRing = new TopicInfo();

    public ProducerMetadata(long refreshBackoffMs,
                            long metadataExpireMs,
                            LogContext logContext,
                            ClusterResourceListeners clusterResourceListeners,
                            Time time) {
        super(refreshBackoffMs, metadataExpireMs, logContext, clusterResourceListeners);
        this.log = logContext.logger(ProducerMetadata.class);
        this.time = time;
    }

    /**
     * Returns whether a topic's metadata should be refreshed. If true, then an optional is
     * returned indicating whether the refresh is urgent, otherwise if false, then a null
     * optional is returned.
     *
     * @param topicInfo the topic info to refresh topic metadata for
     * @param nowMs the approximate current time, in milliseconds
     * @return an optional with whether the refresh is urgent, otherwise null if no refresh is needed
     */
    private Optional<Boolean> shouldRefresh(TopicInfo topicInfo, long nowMs) {
        if (topicInfo.lastRefreshTime() == 0 ||
            topicInfo.lastRefreshTime() + metadataExpireMs() <= nowMs) {
            return Optional.of(true);
        } else if (topicInfo.lastRefreshTime() + metadataExpireMs() / 2 <= nowMs) {
            return Optional.of(false);
        } else {
            return Optional.<Boolean>empty();
        }
    }

    @Override
    public synchronized MetadataRequest.Builder newMetadataRequestBuilder() {
        if ((true))
            return new MetadataRequest.Builder(new ArrayList<>(topics.keySet()), true);

        long nowMs = time.milliseconds();
        List<String> refreshTopics = new ArrayList<>(Math.min(topics.size(), TOPIC_REFRESH_TARGET_SIZE));

        // Add all topics whose metadata refresh is urgent, and non-urgent topics while the list is
        // below the target refresh size.
        TopicInfo topicInfo = refreshRing.refreshRingNext();
        while (topicInfo != refreshRing) {
            Optional<Boolean> refresh = shouldRefresh(topicInfo, nowMs);
            if (!refresh.isPresent()) {
                break;
            }
            if (!refresh.get() && refreshTopics.size() >= TOPIC_REFRESH_TARGET_SIZE) {
                break;
            }
            refreshTopics.add(topicInfo.topic());
            topicInfo = topicInfo.refreshRingNext();
        }

        return new MetadataRequest.Builder(refreshTopics, true);
    }

    private synchronized TopicInfo updateTopicInfo(String topic, TopicInfo topicInfo) {
        if (topicInfo == null) {
            topicInfo = new TopicInfo(topic);
            topicInfo.refreshRingAddAfter(refreshRing);

            requestUpdateForNewTopics();
        }
        topicInfo.updateLastAccessTime(time.milliseconds());
        return topicInfo;
    }

    public synchronized void add(String topic) {
        Objects.requireNonNull(topic, "topic cannot be null");

        topics.compute(topic, (k, v) -> updateTopicInfo(k, v));
    }

    // Visible for testing
    synchronized Set<String> topics() {
        return topics.keySet();
    }

    public synchronized boolean containsTopic(String topic) {
        return topics.containsKey(topic);
    }

    @Override
    public synchronized boolean retainTopic(String topic, boolean isInternal, long nowMs) {
        TopicInfo topicInfo = topics.get(topic);
        if (topicInfo == null) {
            return false;
        }

        long expireMs = topicInfo.lastAccessTime() + TOPIC_EXPIRY_MS;
        if (expireMs > nowMs) {
            return true;
        }

        log.debug("Removing unused topic {} from the metadata list, expireMs {}, now {}", topic, expireMs, nowMs);
        topicInfo.refreshRingRemove();
        topics.remove(topic);
        return false;
    }

    @Override
    public synchronized void topicMetadataUpdated(String topic, long nowMs) {
        TopicInfo topicInfo = topics.get(topic);
        if (topicInfo == null) {
            return;
        }

        topicInfo.updateLastRefreshTime(nowMs);
        topicInfo.refreshRingRemove();
        topicInfo.refreshRingAddAfter(refreshRing.refreshRingPrev());
    }

    @Override
    public synchronized long timeToNextUpdate(long nowMs) {
        long nextUpdateMs = super.timeToNextUpdate(nowMs);
        if (nextUpdateMs == 0) {
            return 0;
        }
        if (topics.isEmpty()) {
            return nextUpdateMs;
        }

        // If the front topic is urgent, then a next update should be issued immediately.
        Optional<Boolean> refresh = shouldRefresh(refreshRing.refreshRingNext(), nowMs);
        if (refresh.orElse(false)) {
            return 0;
        }

        // Determine whether there's enough non-urgent topics that need to be refreshed to
        // warrant performing an update.
        if (topics.size() >= TOPIC_REFRESH_TARGET_SIZE) {
            int ready = 0;
            TopicInfo topicInfo = refreshRing.refreshRingNext();
            while (topicInfo != refreshRing) {
                refresh = shouldRefresh(topicInfo, nowMs);
                if (!refresh.isPresent()) {
                    break;
                }
                ++ready;
                if (ready >= TOPIC_REFRESH_TARGET_SIZE) {
                    return 0;
                }
                topicInfo = topicInfo.refreshRingNext();
            }
        }

        return Math.min(nextUpdateMs, nowMs - refreshRing.refreshRingNext().lastRefreshTime() - metadataExpireMs());
    }

    /**
     * Wait for metadata update until the current version is larger than the last version we know of
     */
    public synchronized void awaitUpdate(final int lastVersion, final long timeoutMs) throws InterruptedException {
        long currentTimeMs = time.milliseconds();
        long deadlineMs = currentTimeMs + timeoutMs < 0 ? Long.MAX_VALUE : currentTimeMs + timeoutMs;
        time.waitObject(this, () -> {
            // Throw fatal exceptions, if there are any. Recoverable topic errors will be handled by the caller.
            maybeThrowFatalException();
            return updateVersion() > lastVersion || isClosed();
        }, deadlineMs);

        if (isClosed())
            throw new KafkaException("Requested metadata update after close");
    }

    @Override
    public synchronized void update(int requestVersion, MetadataResponse response, long now) {
        super.update(requestVersion, response, now);
        notifyAll();
    }

    @Override
    public synchronized void failedUpdate(long now, KafkaException fatalException) {
        super.failedUpdate(now, fatalException);
        if (fatalException != null)
            notifyAll();
    }

    /**
     * Close this instance and notify any awaiting threads.
     */
    @Override
    public synchronized void close() {
        super.close();
        notifyAll();
    }
}
