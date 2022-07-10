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

package org.apache.flink.state.changelog.restore;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.changelog.ChangelogStateBackendHandle;
import org.apache.flink.runtime.state.changelog.ChangelogStateHandle;
import org.apache.flink.runtime.state.changelog.StateChange;
import org.apache.flink.runtime.state.changelog.StateChangelogHandleReader;
import org.apache.flink.runtime.state.changelog.StateChangelogStorageLoader;
import org.apache.flink.state.changelog.ChangelogKeyedStateBackend;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.BiFunctionWithException;
import org.apache.flink.util.function.FunctionWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Restores {@link ChangelogKeyedStateBackend} from the provided {@link ChangelogStateBackendHandle
 * handles}.
 */
@Internal
public class ChangelogBackendRestoreOperation {
    private static final Logger LOG =
            LoggerFactory.getLogger(ChangelogBackendRestoreOperation.class);
    /** Builds base backend for {@link ChangelogKeyedStateBackend} from state. */
    @FunctionalInterface
    public interface BaseBackendBuilder<K>
            extends FunctionWithException<
                    Collection<KeyedStateHandle>, AbstractKeyedStateBackend<K>, Exception> {}

    /** Builds {@link ChangelogRestoreTarget} from the base backend and state. */
    @FunctionalInterface
    public interface ChangelogRestoreTargetBuilder<K>
            extends BiFunctionWithException<
                    AbstractKeyedStateBackend<K>,
                    Collection<ChangelogStateBackendHandle>,
                    ChangelogRestoreTarget<K>,
                    Exception> {}

    public static <K> CheckpointableKeyedStateBackend<K> restore(
            ClassLoader classLoader,
            Collection<ChangelogStateBackendHandle> stateHandles,
            BaseBackendBuilder<K> baseBackendBuilder,
            ChangelogRestoreTargetBuilder<K> changelogRestoreTargetBuilder)
            throws Exception {
        Collection<KeyedStateHandle> baseState = extractBaseState(stateHandles);
        AbstractKeyedStateBackend<K> baseBackend = baseBackendBuilder.apply(baseState);
        ChangelogRestoreTarget<K> changelogRestoreTarget =
                changelogRestoreTargetBuilder.apply(baseBackend, stateHandles);
        long nonMaterializedSize =
                stateHandles.stream()
                        .flatMap(x -> x.getNonMaterializedStateHandles().stream())
                        .mapToLong(x -> x.getStateSize())
                        .sum();
        LOG.info("read changelog handle start, total state size={} .", nonMaterializedSize);
        long currentTime = System.currentTimeMillis();
        for (ChangelogStateBackendHandle handle : stateHandles) {
            if (handle != null) { // null is empty state (no change)
                readBackendHandle(changelogRestoreTarget, handle, classLoader);
            }
        }
        LOG.info(
                "read read changelog handle end, cost {} ms.",
                System.currentTimeMillis() - currentTime);
        return changelogRestoreTarget.getRestoredKeyedStateBackend();
    }

    @SuppressWarnings("unchecked")
    private static <T extends ChangelogStateHandle> void readBackendHandle(
            ChangelogRestoreTarget<?> changelogRestoreTarget,
            ChangelogStateBackendHandle backendHandle,
            ClassLoader classLoader)
            throws Exception {
        Map<Short, StateID> stateIds = new HashMap<>();
        for (ChangelogStateHandle changelogHandle :
                backendHandle.getNonMaterializedStateHandles()) {
            StateChangelogHandleReader<T> changelogHandleReader =
                    (StateChangelogHandleReader<T>)
                            StateChangelogStorageLoader.loadFromStateHandle(changelogHandle)
                                    .createReader();
            try (CloseableIterator<StateChange> changes =
                    changelogHandleReader.getChanges((T) changelogHandle)) {
                while (changes.hasNext()) {
                    ChangelogBackendLogApplier.apply(
                            changes.next(), changelogRestoreTarget, classLoader, stateIds);
                }
            }
        }
    }

    private static Collection<KeyedStateHandle> extractBaseState(
            Collection<ChangelogStateBackendHandle> stateHandles) {
        Preconditions.checkNotNull(stateHandles);
        return stateHandles.stream()
                .filter(Objects::nonNull)
                .map(ChangelogStateBackendHandle::getMaterializedStateHandles)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    private ChangelogBackendRestoreOperation() {}
}
