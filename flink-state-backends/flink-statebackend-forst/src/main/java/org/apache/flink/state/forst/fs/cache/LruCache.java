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
    private final Object lock = new Object();

    @GuardedBy("lock")
    protected CacheLimiter cacheLimiter;

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
                cacheLimiter.release(getValueResource(entry.getValue()));
                return true;
            }
            return false;
        }
    }

    LruCache(int capacity, CacheLimiter cacheLimiter) {
        this.cacheLimiter = cacheLimiter;
        this.dataMap = new LruHashMap(capacity);
    }

    public boolean put(K key, V value) {
        synchronized (lock) {
            V previous = dataMap.put(key, value);
            if (previous != null) {
                internalRemove(previous);
                cacheLimiter.release(getValueResource(previous));
            }
        }
        internalInsert(key, value);
        tryTrim(getValueResource(value));
        synchronized (lock) {
            cacheLimiter.acquire(getValueResource(value));
        }
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
                cacheLimiter.release(getValueResource(previous));
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
    private void tryTrim(long toAddResource) {
        synchronized (lock) {
            if (!cacheLimiter.isOverflow(toAddResource)) { // infinite resource, no need to trim
                return;
            }
            while (cacheLimiter.isOverflow(toAddResource) && !dataMap.isEmpty()) {
                Map.Entry<K, V> toRemove = dataMap.entrySet().iterator().next();
                dataMap.remove(toRemove.getKey());
                internalRemove(toRemove.getValue());
                cacheLimiter.release(getValueResource(toRemove.getValue()));
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
