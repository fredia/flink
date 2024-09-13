/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.v2;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.v2.ReducingState;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.core.state.StateFutureUtils;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * A default implementation of {@link ReducingState} which delegates all async requests to {@link
 * StateRequestHandler}.
 *
 * @param <K> The type of key the state is associated to.
 * @param <V> The type of values kept internally in state.
 */
public class InternalReducingState<K, N, V> extends InternalKeyedState<K, N, V>
        implements ReducingState<V>, InternalMergingState<N> {

    protected final ReduceFunction<V> reduceFunction;

    public InternalReducingState(
            StateRequestHandler stateRequestHandler, ReducingStateDescriptor<V> stateDescriptor) {
        super(stateRequestHandler, stateDescriptor);
        this.reduceFunction = stateDescriptor.getReduceFunction();
    }

    @Override
    public StateFuture<V> asyncGet() {
        return handleRequest(StateRequestType.REDUCING_GET, null);
    }

    @Override
    public StateFuture<Void> asyncAdd(V value) {
        return handleRequest(StateRequestType.REDUCING_GET, null)
                .thenAccept(
                        oldValue -> {
                            V newValue =
                                    oldValue == null
                                            ? value
                                            : reduceFunction.reduce((V) oldValue, value);
                            handleRequest(StateRequestType.REDUCING_ADD, newValue);
                        });
    }

    @Override
    public V get() {
        return handleRequestSync(StateRequestType.REDUCING_GET, null);
    }

    @Override
    public void add(V value) {
        V oldValue = handleRequestSync(StateRequestType.REDUCING_GET, null);
        try {
            V newValue = oldValue == null ? value : reduceFunction.reduce(oldValue, value);
            handleRequestSync(StateRequestType.REDUCING_ADD, newValue);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public StateFuture<Void> asyncMergeNamespaces(N target, Collection<N> sources)
            throws Exception {
        if (sources == null || sources.isEmpty()) {
            return StateFutureUtils.completedVoidFuture();
        }
        // phase 1: read from the sources and target
        List<StateFuture<V>> futures = new ArrayList<>(sources.size() + 1);
        for (N source : sources) {
            if (source != null) {
                setCurrentNamespace(source);
                futures.add(handleRequest(StateRequestType.REDUCING_GET, null));
            }
        }
        setCurrentNamespace(target);
        futures.add(handleRequest(StateRequestType.REDUCING_GET, null));
        // phase 2: merge the sources to the target
        return StateFutureUtils.combineAll(futures)
                .thenCompose(
                        values -> {
                            List<StateFuture<V>> removeFutures = new ArrayList<>(sources.size());
                            V current = null;
                            Iterator<V> valueIterator = values.iterator();
                            for (N source : sources) {
                                V value = valueIterator.next();
                                if (value != null) {
                                    setCurrentNamespace(source);
                                    removeFutures.add(
                                            handleRequest(StateRequestType.REDUCING_REMOVE, null));
                                    if (current != null) {
                                        current = reduceFunction.reduce(current, value);
                                    } else {
                                        current = value;
                                    }
                                }
                            }
                            V targetValue = valueIterator.next();
                            if (current != null && targetValue != null) {
                                current = reduceFunction.reduce(current, targetValue);
                            }
                            V finalCurrent = current;
                            return StateFutureUtils.combineAll(removeFutures)
                                    .thenApply(ignore -> finalCurrent);
                        })
                .thenAccept(
                        currentValue -> {
                            if (currentValue == null) {
                                return;
                            }
                            setCurrentNamespace(target);
                            handleRequest(StateRequestType.REDUCING_ADD, currentValue);
                        });
    }

    @Override
    public void mergeNamespaces(N target, Collection<N> sources) {
        if (sources == null || sources.isEmpty()) {
            return;
        }
        try {
            V current = null;
            // merge the sources to the target
            for (N source : sources) {
                if (source != null) {
                    setCurrentNamespace(source);
                    V oldValue = handleRequestSync(StateRequestType.REDUCING_GET, null);

                    if (oldValue != null) {
                        handleRequestSync(StateRequestType.REDUCING_REMOVE, null);

                        if (current != null) {
                            current = reduceFunction.reduce(current, oldValue);
                        } else {
                            current = oldValue;
                        }
                    }
                }
            }

            // if something came out of merging the sources, merge it or write it to the target
            if (current != null) {
                // create the target full-binary-key
                setCurrentNamespace(target);
                V targetValue = handleRequestSync(StateRequestType.REDUCING_GET, null);

                if (targetValue != null) {
                    current = reduceFunction.reduce(current, targetValue);
                }
                handleRequestSync(StateRequestType.REDUCING_ADD, current);
            }
        } catch (Exception e) {
            throw new RuntimeException("merge namespace fail.", e);
        }
    }
}
