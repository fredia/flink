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

import org.apache.flink.runtime.asyncprocessing.StateExecutor;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestContainer;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.rocksdb.RocksDB;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * The {@link StateExecutor} implementation which executing batch {@link StateRequest}s for
 * ForStStateBackend.
 */
public class ForStStateExecutor implements StateExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(ForStStateExecutor.class);

    /**
     * The coordinator thread which schedules the execution of multiple batches of stateRequests.
     * The number of coordinator threads is 1 to ensure that multiple batches of stateRequests can
     * be executed sequentially.
     */
    private final ExecutorService coordinatorThread;

    /** The worker thread that actually executes the {@link StateRequest}s. */
    private final ThreadPoolExecutor workerThreads;

    private final ThreadPoolExecutor writeThreads = null;

    private final ThreadPoolExecutor iterThreads = null;

    private final RocksDB db;

    private final WriteOptions writeOptions;

    public ForStStateExecutor(
            int ioParallelism, int wio, int iio, RocksDB db, WriteOptions writeOptions) {
        this.coordinatorThread =
                Executors.newSingleThreadExecutor(
                        new ExecutorThreadFactory("ForSt-Coordinator-IO"));
        this.workerThreads =
                (ThreadPoolExecutor)
                        Executors.newFixedThreadPool(
                                ioParallelism, new ExecutorThreadFactory("ForSt-StateExecutor-IO"));
        //        this.writeThreads =
        //                (ThreadPoolExecutor)
        //                        Executors.newFixedThreadPool(
        //                                wio, new ExecutorThreadFactory("ForSt-Write-IO"));
        //        this.iterThreads =
        //                (ThreadPoolExecutor)
        //                        Executors.newFixedThreadPool(
        //                                iio, new ExecutorThreadFactory("ForSt-Iter-IO"));

        this.db = db;
        this.writeOptions = writeOptions;
    }

    @Override
    public CompletableFuture<Integer> executeBatchRequests(
            StateRequestContainer stateRequestContainer) {
        Preconditions.checkArgument(stateRequestContainer instanceof ForStStateRequestClassifier);
        ForStStateRequestClassifier stateRequestClassifier =
                (ForStStateRequestClassifier) stateRequestContainer;
        return CompletableFuture.supplyAsync(
                () -> {
                    List<StateRequest<?, ?, ?>> stateRequests =
                            stateRequestClassifier.pollStateRequests();
                    ForStWriteBatchOperation writeOperations =
                            new ForStWriteBatchOperation(db, null, writeOptions, writeThreads);
                    ForStGeneralMultiGetOperation getOperations =
                            new ForStGeneralMultiGetOperation(db, null, workerThreads);
                    ForStIterateOperation iterOperations =
                            new ForStIterateOperation(db, null, iterThreads);
                    for (StateRequest<?, ?, ?> stateRequest : stateRequests) {
                        stateRequestClassifier.convertStateRequestsToForStDBRequests(
                                stateRequest,
                                writeOperations,
                                getOperations,
                                iterOperations,
                                workerThreads,
                                workerThreads,
                                workerThreads,
                                db);
                    }
                    return stateRequests.size();
                },
                coordinatorThread);

        //        List<CompletableFuture<Void>> futures = new ArrayList<>(3);
        //
        //        List<ForStDBPutRequest<?, ?>> putRequests =
        // stateRequestClassifier.pollDbPutRequests();
        //        if (!putRequests.isEmpty()) {
        //            ForStWriteBatchOperation writeOperations =
        //                    new ForStWriteBatchOperation(db, putRequests, writeOptions,
        // writeThreads);
        //            futures.add(writeOperations.process());
        //        }
        //
        //        List<ForStDBGetRequest<?, ?>> getRequests =
        // stateRequestClassifier.pollDbGetRequests();
        //        if (!getRequests.isEmpty()) {
        //            ForStGeneralMultiGetOperation getOperations =
        //                    new ForStGeneralMultiGetOperation(db, getRequests, workerThreads);
        //            futures.add(getOperations.process());
        //        }
        //
        //        List<ForStDBIterRequest<?>> iterRequests =
        // stateRequestClassifier.pollDbIterRequests();
        //        if (!iterRequests.isEmpty()) {
        //            ForStIterateOperation iterOperations =
        //                    new ForStIterateOperation(db, iterRequests, iterThreads);
        //            futures.add(iterOperations.process());
        //        }

        //        LOG.debug(
        //                "putRequest size {}/{}, getRequest size {}/{}, iter size {}/{}",
        //                putRequests.size(),
        //                writeThreads.getQueue().size(),
        //                getRequests.size(),
        //                workerThreads.getQueue().size(),
        //                iterRequests.size(),
        //                iterThreads.getQueue().size());
        //        LOG.info(
        //                "Executing a batch of state requests, putRequest size {}, getRequest size
        // {}, iter size {}",
        //                putRequests.size(),
        //                getRequests.size(),
        //                iterRequests.size());

        //        coordinatorThread.execute(
        //                () -> {
        //                    long startTime = System.currentTimeMillis();
        //                    List<CompletableFuture<Void>> futures = new ArrayList<>(2);
        //                    List<ForStDBPutRequest<?, ?>> putRequests =
        //                            stateRequestClassifier.pollDbPutRequests();
        //                    if (!putRequests.isEmpty()) {
        //                        ForStWriteBatchOperation writeOperations =
        //                                new ForStWriteBatchOperation(
        //                                        db, putRequests, writeOptions, workerThreads);
        //                        futures.add(writeOperations.process());
        //                    }
        //
        //                    List<ForStDBGetRequest<?, ?>> getRequests =
        //                            stateRequestClassifier.pollDbGetRequests();
        //                    if (!getRequests.isEmpty()) {
        //                        ForStGeneralMultiGetOperation getOperations =
        //                                new ForStGeneralMultiGetOperation(db, getRequests,
        // workerThreads);
        //                        futures.add(getOperations.process());
        //                    }
        //
        //                    List<ForStDBIterRequest<?>> iterRequests =
        //                            stateRequestClassifier.pollDbIterRequests();
        //                    if (!iterRequests.isEmpty()) {
        //                        ForStIterateOperation iterOperations =
        //                                new ForStIterateOperation(db, iterRequests,
        // workerThreads);
        //                        futures.add(iterOperations.process());
        //                    }
        // FutureUtils.combineAll(futures).thenAccept((e) -> resultFuture.complete(null));
        // return resultFuture;
    }

    @Override
    public StateRequestContainer createStateRequestContainer(
            StateRequestHandler stateRequestHandler) {
        return new ForStStateRequestClassifier(stateRequestHandler);
    }

    @Override
    public void shutdown() {
        workerThreads.shutdown();
        // coordinatorThread.shutdown();
        LOG.info("Shutting down the ForStStateExecutor.");
    }
}
