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

import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestContainer;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;

import org.rocksdb.RocksDB;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

/**
 * The ForSt {@link StateRequestContainer} which can classify the state requests by ForStDB
 * requestType (Get、Put or Iterator).
 */
public class ForStStateRequestClassifier implements StateRequestContainer {

    private final StateRequestHandler stateRequestHandler;

    private final List<ForStDBGetRequest<?, ?>> dbGetRequests;

    private final List<ForStDBPutRequest<?, ?>> dbPutRequests;

    private final List<ForStDBIterRequest<?>> dbIterRequests;

    private final List<StateRequest<?, ?, ?>> stateRequests;

    public ForStStateRequestClassifier(StateRequestHandler stateRequestHandler) {
        this.stateRequestHandler = stateRequestHandler;
        this.dbGetRequests = new ArrayList<>();
        this.dbPutRequests = new ArrayList<>();
        this.dbIterRequests = new ArrayList<>();
        this.stateRequests = new ArrayList<>();
    }

    @Override
    public void offer(StateRequest<?, ?, ?> stateRequest) {
        stateRequests.add(stateRequest);
        // convertStateRequestsToForStDBRequests(stateRequest);
    }

    public List<StateRequest<?, ?, ?>> pollStateRequests() {
        return stateRequests;
    }

    @Override
    public boolean isEmpty() {
        return stateRequests.isEmpty();
        // return dbGetRequests.isEmpty() && dbPutRequests.isEmpty() && dbIterRequests.isEmpty();
    }

    @Override
    public int size() {
        return stateRequests.size();
        // return dbGetRequests.size() + dbPutRequests.size() + dbIterRequests.size();
    }

    @SuppressWarnings("ConstantConditions")
    public void convertStateRequestsToForStDBRequests(
            StateRequest<?, ?, ?> stateRequest,
            ForStWriteBatchOperation forStWriteBatchOperation,
            ForStGeneralMultiGetOperation forStGeneralMultiGetOperation,
            ForStIterateOperation forStIterateOperation,
            Executor readExecutor,
            Executor writeExecutor,
            Executor iterateExecutor,
            RocksDB db) {
        StateRequestType stateRequestType = stateRequest.getRequestType();
        switch (stateRequestType) {
            case VALUE_GET:
                {
                    ForStValueState<?, ?> forStValueState =
                            (ForStValueState<?, ?>) stateRequest.getState();
                    forStGeneralMultiGetOperation.processSingle(
                            forStValueState.buildDBGetRequest(stateRequest), readExecutor, db);
                    // dbGetRequests.add(forStValueState.buildDBGetRequest(stateRequest));
                    return;
                }
            case VALUE_UPDATE:
                {
                    ForStValueState<?, ?> forStValueState =
                            (ForStValueState<?, ?>) stateRequest.getState();
                    forStWriteBatchOperation.processSingle(
                            forStValueState.buildDBPutRequest(stateRequest), writeExecutor, db);
                    // dbPutRequests.add(forStValueState.buildDBPutRequest(stateRequest));
                    return;
                }
            case MAP_GET:
            case MAP_CONTAINS:
                {
                    ForStMapState<?, ?, ?> forStMapState =
                            (ForStMapState<?, ?, ?>) stateRequest.getState();
                    forStGeneralMultiGetOperation.processSingle(
                            forStMapState.buildDBGetRequest(stateRequest), readExecutor, db);
                    // dbGetRequests.add(forStMapState.buildDBGetRequest(stateRequest));
                    return;
                }
            case MAP_PUT:
            case MAP_REMOVE:
                {
                    ForStMapState<?, ?, ?> forStMapState =
                            (ForStMapState<?, ?, ?>) stateRequest.getState();
                    forStWriteBatchOperation.processSingle(
                            forStMapState.buildDBPutRequest(stateRequest), writeExecutor, db);
                    // dbPutRequests.add(forStMapState.buildDBPutRequest(stateRequest));
                    return;
                }
            case MAP_ITER:
            case MAP_ITER_KEY:
            case MAP_ITER_VALUE:
            case ITERATOR_LOADING:
                {
                    ForStMapState<?, ?, ?> forStMapState =
                            (ForStMapState<?, ?, ?>) stateRequest.getState();
                    forStIterateOperation.processSingle(
                            forStMapState.buildDBIterRequest(stateRequest), iterateExecutor, db);
                    // dbIterRequests.add(forStMapState.buildDBIterRequest(stateRequest));
                    return;
                }
            case MAP_PUT_ALL:
                {
                    ForStMapState<?, ?, ?> forStMapState =
                            (ForStMapState<?, ?, ?>) stateRequest.getState();
                    forStWriteBatchOperation.processSingle(
                            forStMapState.buildDBBunchPutRequest(stateRequest), writeExecutor, db);
                    // dbPutRequests.add(forStMapState.buildDBBunchPutRequest(stateRequest));
                    return;
                }
            case MAP_IS_EMPTY:
                {
                    ForStMapState<?, ?, ?> forStMapState =
                            (ForStMapState<?, ?, ?>) stateRequest.getState();
                    forStGeneralMultiGetOperation.processSingle(
                            forStMapState.buildDBGetRequest(stateRequest), readExecutor, db);
                    // dbGetRequests.add(forStMapState.buildDBGetRequest(stateRequest));
                    return;
                }
            case CLEAR:
                {
                    if (stateRequest.getState() instanceof ForStValueState) {
                        ForStValueState<?, ?> forStValueState =
                                (ForStValueState<?, ?>) stateRequest.getState();
                        forStWriteBatchOperation.processSingle(
                                forStValueState.buildDBPutRequest(stateRequest), writeExecutor, db);
                        // dbPutRequests.add(forStValueState.buildDBPutRequest(stateRequest));
                        return;
                    } else if (stateRequest.getState() instanceof ForStMapState) {
                        ForStMapState<?, ?, ?> forStMapState =
                                (ForStMapState<?, ?, ?>) stateRequest.getState();
                        forStWriteBatchOperation.processSingle(
                                forStMapState.buildDBBunchPutRequest(stateRequest),
                                writeExecutor,
                                db);
                        // dbPutRequests.add(forStMapState.buildDBBunchPutRequest(stateRequest));
                        return;
                    } else {
                        throw new UnsupportedOperationException(
                                "The State "
                                        + stateRequest.getState().getClass()
                                        + " doesn't yet support the clear method.");
                    }
                }
            default:
                throw new UnsupportedOperationException(
                        "Unsupported state request type:" + stateRequestType);
        }
    }

    public List<ForStDBGetRequest<?, ?>> pollDbGetRequests() {
        return dbGetRequests;
    }

    public List<ForStDBPutRequest<?, ?>> pollDbPutRequests() {
        return dbPutRequests;
    }

    public List<ForStDBIterRequest<?>> pollDbIterRequests() {
        return dbIterRequests;
    }
}
