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

package org.apache.flink.state.forst;

import org.apache.flink.core.state.InternalStateFuture;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler.DisposerCounter;

import org.rocksdb.ColumnFamilyHandle;

import java.io.IOException;

/**
 * The Get access request for ForStDB.
 *
 * @param <K> The type of key in get access request.
 * @param <V> The type of value returned by get request.
 */
public class ForStDBGetRequest<K, V> {

    private final K key;
    private final ForStInnerTable<K, V> table;
    private final InternalStateFuture future;

    private final boolean toBoolean;
    private final boolean checkMapEmpty;

    private int keyGroupPrefixBytes = 1;

    private final DisposerCounter disposer;

    private ForStDBGetRequest(
            K key,
            ForStInnerTable<K, V> table,
            InternalStateFuture future,
            boolean toBoolean,
            boolean checkMapEmpty,
            DisposerCounter disposer) {
        this.key = key;
        this.table = table;
        this.future = future;
        this.toBoolean = toBoolean;
        this.checkMapEmpty = checkMapEmpty;
        this.disposer = disposer;
        if (table instanceof ForStMapState) {
            keyGroupPrefixBytes = ((ForStMapState) table).getKeyGroupPrefixBytes();
        }
    }

    public int getKeyGroupPrefixBytes() {
        return keyGroupPrefixBytes;
    }

    public boolean checkMapEmpty() {
        return checkMapEmpty;
    }

    public byte[] buildSerializedKey() throws IOException {
        return table.serializeKey(key);
    }

    public ColumnFamilyHandle getColumnFamilyHandle() {
        return table.getColumnFamilyHandle();
    }

    @SuppressWarnings("rawtypes")
    public void completeStateFuture(byte[] bytesValue) throws IOException {
        if (disposer != null) {
            disposer.run();
        }
        if (toBoolean) {
            if (checkMapEmpty) {
                ((InternalStateFuture<Boolean>) future).complete(bytesValue == null);
                return;
            }
            ((InternalStateFuture<Boolean>) future).complete(bytesValue != null);
            return;
        }
        if (bytesValue == null) {
            ((InternalStateFuture<V>) future).complete(null);
            return;
        }
        V value = table.deserializeValue(bytesValue);
        ((InternalStateFuture<V>) future).complete(value);
    }

    public void addTime(long t) {
        if (disposer != null) {
            disposer.addTime(t);
        }
    }

    static <K, V> ForStDBGetRequest<K, V> of(
            K key,
            ForStInnerTable<K, V> table,
            InternalStateFuture<V> future,
            DisposerCounter disposer) {
        return new ForStDBGetRequest<>(key, table, future, false, false, disposer);
    }

    @SuppressWarnings("rawtypes")
    static <K, V> ForStDBGetRequest<K, V> of(
            K key,
            ForStInnerTable<K, V> table,
            InternalStateFuture future,
            boolean toBoolean,
            boolean checkMapEmpty,
            DisposerCounter disposer) {
        return new ForStDBGetRequest<>(key, table, future, toBoolean, checkMapEmpty, disposer);
    }
}
