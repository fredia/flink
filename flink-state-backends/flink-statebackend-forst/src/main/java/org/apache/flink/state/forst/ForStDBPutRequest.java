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

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Map;

/**
 * The Put access request for ForStDB.
 *
 * @param <K> The type of key in put access request.
 * @param <V> The type of value in put access request.
 */
public class ForStDBPutRequest<K, V> {

    protected final K key;

    @Nullable protected final V value;

    protected final ForStInnerTable<K, V> table;

    protected final InternalStateFuture<Void> future;

    protected final boolean tableIsMap;

    private DisposerCounter disposer;

    protected ForStDBPutRequest(
            K key,
            V value,
            ForStInnerTable<K, V> table,
            InternalStateFuture<Void> future,
            DisposerCounter disposer) {
        this.key = key;
        this.value = value;
        this.table = table;
        this.future = future;
        this.tableIsMap = table instanceof ForStMapState;
        this.disposer = disposer;
    }

    public boolean valueIsNull() {
        return value == null;
    }

    public boolean valueIsMap() {
        return value instanceof Map;
    }

    public ColumnFamilyHandle getColumnFamilyHandle() {
        return table.getColumnFamilyHandle();
    }

    public byte[] buildSerializedKey() throws IOException {
        return table.serializeKey(key);
    }

    public byte[] buildSerializedValue() throws IOException {
        assert value != null;
        return table.serializeValue(value);
    }

    public void completeStateFuture() {
        if (disposer != null) {
            disposer.run();
        }
        future.complete(null);
    }

    public void countTime(long t) {
        if (disposer != null) {
            disposer.addTime(t);
        }
    }

    /**
     * If the value of the ForStDBPutRequest is null, then the request will signify the deletion of
     * the data associated with that key.
     */
    static <K, V> ForStDBPutRequest<K, V> of(
            K key,
            @Nullable V value,
            ForStInnerTable<K, V> table,
            InternalStateFuture<Void> future,
            DisposerCounter disposer) {
        return new ForStDBPutRequest<>(key, value, table, future, disposer);
    }
}
