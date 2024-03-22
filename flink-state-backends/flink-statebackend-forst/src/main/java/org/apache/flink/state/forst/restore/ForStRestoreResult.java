/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.forst.restore;

import org.apache.flink.state.forst.ForStNativeMetricMonitor;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;

import javax.annotation.Nullable;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Entity holding result of ForSt instance restore. */
public class ForStRestoreResult {
    private final RocksDB db;
    private final ColumnFamilyHandle defaultColumnFamilyHandle;
    private final ForStNativeMetricMonitor nativeMetricMonitor;

    private final CompletableFuture<Void> asyncCompactAfterRestoreFuture;

    public ForStRestoreResult(
            RocksDB db,
            ColumnFamilyHandle defaultColumnFamilyHandle,
            ForStNativeMetricMonitor nativeMetricMonitor,
            @Nullable CompletableFuture<Void> asyncCompactAfterRestoreFuture) {
        this.db = db;
        this.defaultColumnFamilyHandle = defaultColumnFamilyHandle;
        this.nativeMetricMonitor = nativeMetricMonitor;
        this.asyncCompactAfterRestoreFuture = asyncCompactAfterRestoreFuture;
    }

    public RocksDB getDb() {
        return db;
    }

    public ColumnFamilyHandle getDefaultColumnFamilyHandle() {
        return defaultColumnFamilyHandle;
    }

    public ForStNativeMetricMonitor getNativeMetricMonitor() {
        return nativeMetricMonitor;
    }

    public Optional<CompletableFuture<Void>> getAsyncCompactAfterRestoreFuture() {
        return Optional.ofNullable(asyncCompactAfterRestoreFuture);
    }
}
