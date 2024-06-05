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

import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.api.common.state.v2.StateIterator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.state.InternalStateFuture;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.state.forst.ForStDBIterRequest.ResultType;

import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDB;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.state.forst.ForStIterateOperation.CACHE_SIZE_LIMIT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/** Test for {@link ForStIterateOperation}. */
class ForStDBIterateOperationTest extends ForStDBOperationTestBase {

    @Test
    void testIterateValues() throws Exception {
        ForStMapState<Integer, String, String> mapState = buildForStMapState("map-iter");
        prepareData(10, mapState, db);
        TestStateFuture<StateIterator<String>> future = new TestStateFuture<>();
        List<ForStDBIterRequest<?>> batchIterRequest = new ArrayList<>();
        ContextKey<Integer> contextKey = buildContextKey(1);
        ForStDBIterRequest<String> request1 =
                new ForStDBIterRequest<>(
                        ResultType.VALUE, contextKey, mapState, null, future, null, null);
        batchIterRequest.add(request1);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        ForStIterateOperation iterOperation =
                new ForStIterateOperation(db, batchIterRequest, executor);
        iterOperation.process().get();

        StateIterator<String> iterator = future.getCompletedResult();
        AtomicInteger count = new AtomicInteger(0);
        iterator.onNext(
                val -> {
                    assertThat(val).isEqualTo("val-" + count.getAndIncrement());
                });
        assertThat(count.get()).isEqualTo(10);
        assertThat(iterator.isEmpty()).isFalse();
    }

    @Test
    void testIterateKeys() throws Exception {
        ForStMapState<Integer, String, String> mapState = buildForStMapState("map-iter");
        prepareData(13, mapState, db);
        TestStateFuture<StateIterator<String>> future = new TestStateFuture<>();
        List<ForStDBIterRequest<?>> batchIterRequest = new ArrayList<>();
        ContextKey<Integer> contextKey = buildContextKey(1);
        ForStDBIterRequest<String> request1 =
                new ForStDBIterRequest<>(
                        ResultType.KEY, contextKey, mapState, null, future, null, null);
        batchIterRequest.add(request1);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        ForStIterateOperation iterOperation =
                new ForStIterateOperation(db, batchIterRequest, executor);
        iterOperation.process().get();

        StateIterator<String> iterator = future.getCompletedResult();
        AtomicInteger count = new AtomicInteger(0);
        iterator.onNext(
                val -> {
                    assertThat(val).isEqualTo("uk-" + count.getAndIncrement());
                });
        assertThat(count.get()).isEqualTo(13);
        assertThat(iterator.isEmpty()).isFalse();
    }

    @Test
    void testIterateEntries() throws Exception {
        ForStMapState<Integer, String, String> mapState = buildForStMapState("map-iter");
        prepareData(3, mapState, db);
        TestStateFuture<StateIterator<Map.Entry<String, String>>> future = new TestStateFuture<>();
        List<ForStDBIterRequest<?>> batchIterRequest = new ArrayList<>();
        ContextKey<Integer> contextKey = buildContextKey(1);
        ForStDBIterRequest<Map.Entry<String, String>> request1 =
                new ForStDBIterRequest<>(
                        ResultType.ENTRY, contextKey, mapState, null, future, null, null);
        batchIterRequest.add(request1);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        ForStIterateOperation iterOperation =
                new ForStIterateOperation(db, batchIterRequest, executor);
        iterOperation.process().get();

        StateIterator<Map.Entry<String, String>> iterator = future.getCompletedResult();
        AtomicInteger count = new AtomicInteger(0);
        iterator.onNext(
                entry -> {
                    int cnt = count.getAndIncrement();
                    assertThat(entry.getKey()).isEqualTo("uk-" + cnt);
                    assertThat(entry.getValue()).isEqualTo("val-" + cnt);
                });
        assertThat(count.get()).isEqualTo(3);
        assertThat(iterator.isEmpty()).isFalse();
    }

    @Test
    void testIteratorLoading() throws Exception {
        ForStMapState<Integer, String, String> mapState = buildForStMapState("map-iter");
        prepareData(200, mapState, db);
        TestStateFuture<StateIterator<Map.Entry<String, String>>> future = new TestStateFuture<>();
        List<ForStDBIterRequest<?>> batchIterRequest = new ArrayList<>();
        ContextKey<Integer> contextKey = buildContextKey(1);
        MockStateRequestHandler stateRequestHandler = new MockStateRequestHandler();
        ForStDBIterRequest<Map.Entry<String, String>> request1 =
                new ForStDBIterRequest<>(
                        ResultType.ENTRY,
                        contextKey,
                        mapState,
                        stateRequestHandler,
                        future,
                        null,
                        null);
        batchIterRequest.add(request1);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        ForStIterateOperation iterOperation =
                new ForStIterateOperation(db, batchIterRequest, executor);
        iterOperation.process().get();

        StateIterator<Map.Entry<String, String>> iterator = future.getCompletedResult();
        AtomicInteger count = new AtomicInteger(0);
        try {
            iterator.onNext(
                    entry -> {
                        int cnt = count.getAndIncrement();
                        assertThat(entry.getKey()).isEqualTo("uk-" + cnt);
                        assertThat(entry.getValue()).isEqualTo("val-" + cnt);
                    });
            fail("should throw NPE");
        } catch (NullPointerException npe) {
            assertThat(stateRequestHandler.payload).isNotNull();
            assertThat(count.get()).isEqualTo(CACHE_SIZE_LIMIT);
            Tuple2<ResultType, byte[]> tuple =
                    (Tuple2<ResultType, byte[]>) stateRequestHandler.payload;
            assertThat(tuple.f0).isEqualTo(ResultType.ENTRY);
            TestStateFuture<StateIterator<Map.Entry<String, String>>> future2 =
                    new TestStateFuture<>();
            ForStDBIterRequest<Map.Entry<String, String>> request2 =
                    new ForStDBIterRequest<>(
                            tuple.f0, contextKey, mapState, null, future2, tuple.f1, null);
            batchIterRequest.clear();
            batchIterRequest.add(request2);
            ForStIterateOperation iterOperation2 =
                    new ForStIterateOperation(db, batchIterRequest, executor);
            iterOperation2.process().get();
            StateIterator<Map.Entry<String, String>> iterator2 = future2.getCompletedResult();
            iterator2.onNext(
                    entry -> {
                        int cnt = count.getAndIncrement();
                        assertThat(entry.getKey()).isEqualTo("uk-" + cnt);
                        assertThat(entry.getValue()).isEqualTo("val-" + cnt);
                    });
            assertThat(count.get()).isEqualTo(200);
            assertThat(iterator2.isEmpty()).isFalse();
        }
    }

    private static void prepareData(
            int num, ForStMapState<Integer, String, String> mapState, RocksDB db) throws Exception {
        for (int i = 0; i < num; i++) {
            ContextKey<Integer> contextKey = buildContextKey(1);
            contextKey.setUserKey("uk-" + i);
            String value = "val-" + i;
            byte[] keyBytes = mapState.serializeKey(contextKey);
            byte[] valueBytes = mapState.serializeValue(value);
            db.put(mapState.getColumnFamilyHandle(), keyBytes, valueBytes);
        }
    }

    private static class MockStateRequestHandler implements StateRequestHandler {
        Object payload = null;

        @Override
        public <IN, OUT> InternalStateFuture<OUT> handleRequest(
                @Nullable State state, StateRequestType type, @Nullable IN payload) {
            assertThat(type).isEqualTo(StateRequestType.ITERATOR_LOADING);
            this.payload = payload;
            return null;
        }

        @Override
        public Runnable getDisposer() {
            return null;
        }
    }
}
