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

import java.util.Objects;

public class TopicInfo {
    // The topic's name, or null if this is the sentinel root topic for the ProducerMetadata.
    public final String topic;

    // The approximate time when the topic's metadata was last accessed. This is used for removing
    // cached metadata if a topic hasn't been accessed for a defined amount of time.
    private long lastAccessTime = 0;

    // The approximate time when the topic's metadata was last refreshed from a broker. This is used
    // for determining if/when a topic's metadata should be refreshed.
    private long lastRefreshTime = 0;

    // The previous/next topic info in the refresh ring.
    private TopicInfo refreshRingPrev = null;
    private TopicInfo refreshRingNext = null;

    /**
     * Constructs a sentinel topic info that can be used as a refresh list root. The topic for
     * the info will always be null.
     */
    public TopicInfo() {
        this.topic = null;
        this.refreshRingNext = this;
        this.refreshRingPrev = this;
    }

    /**
     * Constructs the topic info for the provided topic.
     *
     * @param topic the topic to track information for
     */
    public TopicInfo(String topic) {
        Objects.requireNonNull(topic, "topic cannot be null");
        this.topic = topic;
    }

    /**
     * The topic whose information is being tracked.
     *
     * @return the topic whose information is being tracked
     */
    public String topic() {
        return this.topic;
    }

    /**
     * The approximate time when the topic was last accessed.
     *
     * @return the approximate time, in milliseconds, when the topic was last accessed
     */
    public long lastAccessTime() {
        return this.lastAccessTime;
    }

    /**
     * Updates the last access time for the topic, which should be the approximate current time.
     *
     * @param nowMs the approximate current time, in milliseconds
     */
    public void updateLastAccessTime(long nowMs) {
        this.lastAccessTime = nowMs;
    }

    /**
     * The approximate time when the topic's metadata was last refreshed.
     *
     * @return the approximate time, in milliseconds, when the topic's metadata was last refreshed
     */
    public long lastRefreshTime() {
        return this.lastRefreshTime;
    }

    /**
     * Updates the last refresh time for the topic's metadata, which should be the approximate current time.
     *
     * @param nowMs the approximate current time, in milliseconds
     */
    public void updateLastRefreshTime(long nowMs) {
        this.lastRefreshTime = nowMs;
    }

    public TopicInfo refreshRingNext() {
        return this.refreshRingNext;
    }

    public TopicInfo refreshRingPrev() {
        return this.refreshRingPrev;
    }

    public void refreshRingAddAfter(TopicInfo after) {
        assert(after != this);
        Objects.requireNonNull(after, "topic info `after` cannot be null");

        if (this.refreshRingNext != null) {
            refreshRingRemove();
        }

        if (this == after) {
            this.refreshRingNext = this;
            this.refreshRingPrev = this;
        } else {
            this.refreshRingNext = after.refreshRingNext;
            this.refreshRingPrev = after;
            after.refreshRingNext = this;
        }
    }

    public void refreshRingRemove() {
        this.refreshRingNext.refreshRingPrev = this.refreshRingPrev;
        this.refreshRingPrev.refreshRingNext = this.refreshRingNext;
        this.refreshRingNext = null;
        this.refreshRingPrev = null;
    }
}
