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

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The general-purpose multiGet operation implementation for ForStDB, which simulates multiGet by
 * calling the Get API multiple times with multiple threads.
 */
public class ForStGeneralMultiGetOperation implements ForStDBOperation {

    private final RocksDB db;

    private final List<ForStDBGetRequest<?, ?, ?, ?>> batchRequest;

    private final Executor executor;

    ForStGeneralMultiGetOperation(
            RocksDB db, List<ForStDBGetRequest<?, ?, ?, ?>> batchRequest, Executor executor) {
        this.db = db;
        this.batchRequest = batchRequest;
        this.executor = executor;
    }

    @Override
    public CompletableFuture<Void> process() {
        // TODO: Use MultiGet to optimize this implement
        CompletableFuture<Void> future = new CompletableFuture<>();

        AtomicReference<Exception> error = new AtomicReference<>();
        AtomicInteger counter = new AtomicInteger(3);
        for (int p = 0; p < 3; p++) {
            int startIndex = batchRequest.size() * p / 3;
            int endIndex = batchRequest.size() * (p + 1) / 3;
            if (startIndex >= endIndex) {
                if (counter.decrementAndGet() == 0
                        && !future.isCompletedExceptionally()) {
                    future.complete(null);
                }
                continue;
            }
            executor.execute(
                    () -> {
                        ReadOptions readOptions = new ReadOptions();
                        readOptions.setReadaheadSize(0);

                        List<byte[]> keys = new ArrayList<>(endIndex - startIndex);
                        List<ColumnFamilyHandle> columnFamilyHandles =
                                new ArrayList<>(endIndex - startIndex);
                        try {
                            for (int i = startIndex; i < endIndex; i++) {
                                ForStDBGetRequest<?, ?, ?, ?> request = batchRequest.get(i);
                                byte[] key = request.buildSerializedKey();
                                keys.add(key);
                                columnFamilyHandles.add(request.getColumnFamilyHandle());
                            }
                            List<byte[]> values =
                                    db.multiGetAsList(readOptions, columnFamilyHandles, keys);
                            for (int i = startIndex; i < endIndex; i++) {
                                ForStDBGetRequest<?, ?, ?, ?> request = batchRequest.get(i);
                                request.completeStateFuture(values.get(i - startIndex));
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        } finally {
                            if (counter.decrementAndGet() == 0
                                    && !future.isCompletedExceptionally()) {
                                future.complete(null);
                            }
                        }
                    });
        }
        return future;

        //        CompletableFuture<Void> future = new CompletableFuture<>();
        //
        //        AtomicReference<Exception> error = new AtomicReference<>();
        //        AtomicInteger counter = new AtomicInteger(batchRequest.size());
        //        for (int i = 0; i < batchRequest.size(); i++) {
        //            ForStDBGetRequest<?, ?, ?, ?> request = batchRequest.get(i);
        //            executor.execute(
        //                    () -> {
        //                        try {
        //                            if (error.get() == null) {
        //                                request.process(db);
        //                            } else {
        //                                request.completeStateFutureExceptionally(
        //                                        "Error already occurred in other state request of
        // the same "
        //                                                + "group, failed the state request
        // directly",
        //                                        error.get());
        //                            }
        //                        } catch (Exception e) {
        //                            error.set(e);
        //                            request.completeStateFutureExceptionally(
        //                                    "Error when execute ForStDb get operation", e);
        //                            future.completeExceptionally(e);
        //                        } finally {
        //                            if (counter.decrementAndGet() == 0
        //                                    && !future.isCompletedExceptionally()) {
        //                                future.complete(null);
        //                            }
        //                        }
        //                    });
        //        }
        //        return future;
    }
}
