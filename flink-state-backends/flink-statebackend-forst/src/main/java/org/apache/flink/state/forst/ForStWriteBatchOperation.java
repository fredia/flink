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
import org.rocksdb.RocksDB;
import org.rocksdb.WriteOptions;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

/** The writeBatch operation implementation for ForStDB. */
public class ForStWriteBatchOperation implements ForStDBOperation {

    private final RocksDB db;

    private final List<ForStDBPutRequest<?, ?>> batchRequest;

    private final WriteOptions writeOptions;

    private final Executor executor;

    ForStWriteBatchOperation(
            RocksDB db,
            List<ForStDBPutRequest<?, ?>> batchRequest,
            WriteOptions writeOptions,
            Executor executor) {
        this.db = db;
        this.batchRequest = batchRequest;
        this.writeOptions = writeOptions;
        this.executor = executor;
    }

    @Override
    public CompletableFuture<Void> process() {
        return CompletableFuture.runAsync(
                () -> {
                    try (ForStDBWriteBatchWrapper writeBatch =
                            new ForStDBWriteBatchWrapper(db, writeOptions, batchRequest.size())) {
                        for (ForStDBPutRequest<?, ?> request : batchRequest) {
                            ColumnFamilyHandle cf = request.getColumnFamilyHandle();
                            if (request.valueIsNull()) {
                                if (request instanceof ForStDBBunchPutRequest) {
                                    ForStDBBunchPutRequest<?> bunchPutRequest =
                                            (ForStDBBunchPutRequest<?>) request;
                                    byte[] primaryKey = bunchPutRequest.buildSerializedKey(null);
                                    byte[] endKey = ForStDBBunchPutRequest.nextBytes(primaryKey);

                                    // use deleteRange delete all records under the primary key
                                    db.deleteRange(
                                            request.getColumnFamilyHandle(), primaryKey, endKey);
                                } else {
                                    // put(key, null) == delete(key)
                                    writeBatch.remove(
                                            request.getColumnFamilyHandle(),
                                            request.buildSerializedKey());
                                }
                            } else if (!request.valueIsMap()) {
                                byte[] key = request.buildSerializedKey();
                                byte[] value = request.buildSerializedValue();
                                writeBatch.put(cf, key, value);
                            } else {
                                ForStDBBunchPutRequest<?> bunchPutRequest =
                                        (ForStDBBunchPutRequest<?>) request;

                                for (Map.Entry<?, ?> entry :
                                        bunchPutRequest.getBunchValue().entrySet()) {
                                    byte[] key = bunchPutRequest.buildSerializedKey(entry.getKey());
                                    byte[] value =
                                            bunchPutRequest.buildSerializedValue(entry.getValue());
                                    writeBatch.put(cf, key, value);
                                }
                            }
                        }
                        writeBatch.flush();
                        for (ForStDBPutRequest<?, ?> request : batchRequest) {
                            request.completeStateFuture();
                        }
                    } catch (Exception e) {
                        throw new CompletionException("Error while adding data to ForStDB", e);
                    }
                },
                executor);
    }
}
