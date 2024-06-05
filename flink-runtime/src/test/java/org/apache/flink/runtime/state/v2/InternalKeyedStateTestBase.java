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

package org.apache.flink.runtime.state.v2;

import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.runtime.asyncprocessing.AsyncExecutionController;
import org.apache.flink.runtime.asyncprocessing.StateExecutor;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestContainer;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.runtime.mailbox.SyncMailboxExecutor;
import org.apache.flink.runtime.state.AsyncKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.StateBackend;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Tests for the common/shared functionality of {@link InternalKeyedState}. */
public class InternalKeyedStateTestBase {

    @SuppressWarnings({"rawtypes"})
    AsyncExecutionController aec;

    TestStateExecutor testStateExecutor;

    AtomicReference<Throwable> exception;

    @BeforeEach
    void setup() {
        testStateExecutor = (TestStateExecutor) createStateExecutor();
        aec =
                new AsyncExecutionController<>(
                        new SyncMailboxExecutor(),
                        (a, b) -> {
                            exception.set(b);
                        },
                        testStateExecutor,
                        1,
                        1,
                        1000,
                        1,
                        null);
        exception = new AtomicReference<>(null);
    }

    @AfterEach
    void after() {
        assertThat(exception.get()).isNull();
    }

    private StateExecutor createStateExecutor() {
        TestAsyncStateBackend testAsyncStateBackend = new TestAsyncStateBackend();
        assertThat(testAsyncStateBackend.supportsAsyncKeyedStateBackend()).isTrue();
        return testAsyncStateBackend.createAsyncKeyedStateBackend(null).createStateExecutor();
    }

    <IN> void validateRequestRun(
            @Nullable State state, StateRequestType type, @Nullable IN payload) {
        aec.triggerIfNeeded(true);
        testStateExecutor.validate(state, type, payload);
        assertThat(testStateExecutor.receivedRequest.isEmpty()).isTrue();
    }

    /**
     * A brief implementation of {@link StateBackend} which illustrates the interaction between AEC
     * and StateBackend.
     */
    static class TestAsyncStateBackend implements StateBackend {

        @Override
        public <K> CheckpointableKeyedStateBackend<K> createKeyedStateBackend(
                KeyedStateBackendParameters<K> parameters) throws Exception {
            throw new UnsupportedOperationException("Don't support createKeyedStateBackend yet");
        }

        @Override
        public OperatorStateBackend createOperatorStateBackend(
                OperatorStateBackendParameters parameters) throws Exception {
            throw new UnsupportedOperationException("Don't support createOperatorStateBackend yet");
        }

        @Override
        public boolean supportsAsyncKeyedStateBackend() {
            return true;
        }

        @Override
        public <K> AsyncKeyedStateBackend createAsyncKeyedStateBackend(
                KeyedStateBackendParameters<K> parameters) {
            return new AsyncKeyedStateBackend() {
                @Override
                public void close() throws IOException {}

                @Override
                public void setup(@Nonnull StateRequestHandler stateRequestHandler) {}

                @Nonnull
                @Override
                public <SV, S extends State> S createState(@Nonnull StateDescriptor<SV> stateDesc)
                        throws Exception {
                    return null;
                }

                @Override
                public StateExecutor createStateExecutor() {
                    return new TestStateExecutor();
                }

                @Override
                public void dispose() {
                    // do nothing
                }
            };
        }
    }

    /**
     * A brief implementation of {@link StateExecutor}, to illustrate the interaction between AEC
     * and StateExecutor.
     */
    static class TestStateExecutor implements StateExecutor {

        private Deque<StateRequest<?, ?, ?>> receivedRequest;

        TestStateExecutor() {
            receivedRequest = new ConcurrentLinkedDeque<>();
        }

        <IN> void validate(@Nullable State state, StateRequestType type, @Nullable IN payload) {
            assertThat(receivedRequest.isEmpty()).isFalse();
            StateRequest<?, ?, ?> request = receivedRequest.pop();
            assertThat(request.getState()).isEqualTo(state);
            assertThat(request.getRequestType()).isEqualTo(type);
            assertThat(request.getPayload()).isEqualTo(payload);
        }

        @Override
        public CompletableFuture<Void> executeBatchRequests(
                StateRequestContainer stateRequestContainer) {
            receivedRequest.addAll(((TestStateRequestContainer) stateRequestContainer).requests);
            CompletableFuture<Void> future = new CompletableFuture<>();
            future.complete(null);
            return future;
        }

        @Override
        public StateRequestContainer createStateRequestContainer(StateRequestHandler handler) {
            return new TestStateRequestContainer();
        }

        @Override
        public void shutdown() {}

        static class TestStateRequestContainer implements StateRequestContainer {
            ArrayList<StateRequest<?, ?, ?>> requests = new ArrayList<>();

            @Override
            public void offer(StateRequest<?, ?, ?> stateRequest) {
                requests.add(stateRequest);
            }

            @Override
            public boolean isEmpty() {
                return requests.isEmpty();
            }

            @Override
            public int size() {
                return requests.size();
            }
        }
    }
}
