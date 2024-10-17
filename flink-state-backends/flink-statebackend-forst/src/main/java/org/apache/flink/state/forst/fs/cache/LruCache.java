/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.forst.fs.cache;

import javax.annotation.concurrent.GuardedBy;

import java.io.Closeable;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Uniformed LRU Cache.
 *
 * @param <K> key type.
 * @param <V> value type.
 */
abstract class LruCache<K, V> implements Closeable {
    /** Total available resource. */
    protected long resource;

    private final Object lock = new Object();

    /** Current occupied resource. */
    @GuardedBy("lock")
    protected long currentResource;

    @GuardedBy("lock")
    private final LruHashMap dataMap;

    /** Internal underlying data map. */
    class LruHashMap extends LinkedHashMap<K, V> {

        private static final int DEFAULT_SIZE = 1024;

        /** Maximum capacity. */
        private final int capacity;

        LruHashMap(int capacity) {
            super(DEFAULT_SIZE, 0.75f, true);
            this.capacity = capacity;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<K, V> entry) {
            if (capacity > 0 && size() > capacity) {
                internalRemove(entry.getValue());
                currentResource -= getValueResource(entry.getValue());
                return true;
            }
            return false;
        }
    }

    LruCache(int capacity, long resource) {
        this.resource = resource;
        this.dataMap = new LruHashMap(capacity);
        this.currentResource = 0;
    }

    public boolean put(K key, V value) {
        synchronized (lock) {
            V previous = dataMap.put(key, value);
            if (previous != null) {
                internalRemove(previous);
                currentResource -= getValueResource(previous);
            }
        }
        internalInsert(key, value);
        synchronized (lock) {
            currentResource += getValueResource(value);
        }
        tryTrim();
        return false;
    }

    public V get(K key) {
        synchronized (lock) {
            V value = dataMap.get(key);
            return internalGet(key, value);
        }
    }

    public V remove(K key) {
        synchronized (lock) {
            V previous = dataMap.remove(key);
            if (previous != null) {
                internalRemove(previous);
                currentResource -= getValueResource(previous);
            }
            return previous;
        }
    }

    public int getSize() {
        synchronized (lock) {
            return dataMap.size();
        }
    }

    /**
     * Try to evict the old entries in cache until the current occupied resource is less than the
     * resource.
     */
    private void tryTrim() {
        if (resource <= 0) { // infinite resource, no need to trim
            return;
        }
        synchronized (lock) {
            while (currentResource > resource && !dataMap.isEmpty()) {
                Map.Entry<K, V> toRemove = dataMap.entrySet().iterator().next();
                dataMap.remove(toRemove.getKey());
                internalRemove(toRemove.getValue());
                currentResource -= getValueResource(toRemove.getValue());
            }
        }
    }

    @Override
    public void close() {
        synchronized (lock) {
            for (V value : dataMap.values()) {
                internalRemove(value);
            }
            dataMap.clear();
        }
    }

    abstract V internalGet(K key, V value);

    abstract void internalInsert(K key, V value);

    abstract void internalRemove(V value);

    abstract long getValueResource(V value);
}
