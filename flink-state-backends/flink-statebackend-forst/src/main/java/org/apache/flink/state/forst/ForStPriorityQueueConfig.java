/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.forst;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.state.forst.ForStStateBackend.PriorityQueueStateType;

import javax.annotation.Nullable;

import java.io.Serializable;

import static org.apache.flink.state.forst.ForStOptions.FORST_TIMER_SERVICE_FACTORY_CACHE_SIZE;
import static org.apache.flink.state.forst.ForStOptions.TIMER_SERVICE_FACTORY;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** The configuration of ForSt priority queue state implementation. */
public class ForStPriorityQueueConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final int UNDEFINED_FORST_PRIORITY_QUEUE_SET_CACHE_SIZE = -1;

    /** This determines the type of priority queue state. */
    private @Nullable PriorityQueueStateType priorityQueueStateType;

    /** cache size per keyGroup for ForSt priority queue state. */
    private int forStPriorityQueueSetCacheSize;

    public ForStPriorityQueueConfig() {
        this(null, UNDEFINED_FORST_PRIORITY_QUEUE_SET_CACHE_SIZE);
    }

    public ForStPriorityQueueConfig(
            PriorityQueueStateType priorityQueueStateType, int forStPriorityQueueSetCacheSize) {
        this.priorityQueueStateType = priorityQueueStateType;
        this.forStPriorityQueueSetCacheSize = forStPriorityQueueSetCacheSize;
    }

    /**
     * Gets the type of the priority queue state. It will fall back to the default value if it is
     * not explicitly set.
     */
    public PriorityQueueStateType getPriorityQueueStateType() {
        return priorityQueueStateType == null
                ? TIMER_SERVICE_FACTORY.defaultValue()
                : priorityQueueStateType;
    }

    public void setPriorityQueueStateType(PriorityQueueStateType type) {
        this.priorityQueueStateType = checkNotNull(type);
    }

    /**
     * Gets the cache size of ForSt priority queue set. It will fall back to the default value if it
     * is not explicitly set.
     */
    public int getForStPriorityQueueSetCacheSize() {
        return forStPriorityQueueSetCacheSize == UNDEFINED_FORST_PRIORITY_QUEUE_SET_CACHE_SIZE
                ? FORST_TIMER_SERVICE_FACTORY_CACHE_SIZE.defaultValue()
                : forStPriorityQueueSetCacheSize;
    }

    public static ForStPriorityQueueConfig fromOtherAndConfiguration(
            ForStPriorityQueueConfig other, ReadableConfig config) {
        PriorityQueueStateType priorityQueueType =
                (null == other.priorityQueueStateType)
                        ? config.get(TIMER_SERVICE_FACTORY)
                        : other.priorityQueueStateType;
        int cacheSize =
                (other.forStPriorityQueueSetCacheSize
                                == UNDEFINED_FORST_PRIORITY_QUEUE_SET_CACHE_SIZE)
                        ? config.get(FORST_TIMER_SERVICE_FACTORY_CACHE_SIZE)
                        : other.forStPriorityQueueSetCacheSize;
        return new ForStPriorityQueueConfig(priorityQueueType, cacheSize);
    }

    public static ForStPriorityQueueConfig buildWithPriorityQueueType(PriorityQueueStateType type) {
        return new ForStPriorityQueueConfig(
                type, FORST_TIMER_SERVICE_FACTORY_CACHE_SIZE.defaultValue());
    }
}
