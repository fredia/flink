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

package org.apache.flink.runtime.state;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

import static org.apache.flink.util.Preconditions.checkState;

/** This registry manages handles which is written for local recovery. */
public class LocalStateRegistry implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(LocalStateRegistry.class);
    /**
     * All registered handles. (handle,checkpointID) represents a handle and the latest checkpoint
     * that refer to this handle.
     */
    private final Map<StreamStateHandle, Long> registeredHandles;

    /** This flag indicates whether the registry is open or if close() was called. */
    private boolean open;

    /** Executor for async state deletion */
    private final Executor asyncDisposalExecutor;

    public LocalStateRegistry(Executor ioExecutor) {
        this.registeredHandles = new HashMap<>();
        this.asyncDisposalExecutor = ioExecutor;
        this.open = true;
    }

    public StreamStateHandle register(StreamStateHandle handle, long checkpointID) {
        synchronized (registeredHandles) {
            checkState(open, "Attempt to register state to closed LocalStateRegistry.");
            if (registeredHandles.containsKey(handle)) {
                long pre = registeredHandles.get(handle);
                if (checkpointID > pre) {
                    registeredHandles.put(handle, checkpointID);
                }
            } else {
                registeredHandles.put(handle, checkpointID);
            }
        }
        return handle;
    }

    public void unRegister(long upTo) {
        List<StreamStateHandle> handles = new ArrayList<>();
        synchronized (registeredHandles) {
            Iterator<Entry<StreamStateHandle, Long>> iterator =
                    registeredHandles.entrySet().iterator();
            while (iterator.hasNext()) {
                Entry<StreamStateHandle, Long> entry = iterator.next();
                if (entry.getValue() < upTo) {
                    handles.add(entry.getKey());
                    iterator.remove();
                }
            }
        }
        for (StreamStateHandle handle : handles) {
            scheduleAsyncDelete(handle);
        }
    }

    private void scheduleAsyncDelete(StreamStateHandle streamStateHandle) {
        if (streamStateHandle != null) {
            LOG.trace("Scheduled delete of state handle {}.", streamStateHandle);
            Runnable discardRunner =
                    () -> {
                        try {
                            streamStateHandle.discardState();
                        } catch (Exception exception) {
                            LOG.warn(
                                    "A problem occurred during asynchronous disposal of a stream handle {}.",
                                    streamStateHandle);
                        }
                    };
            try {
                asyncDisposalExecutor.execute(discardRunner);
            } catch (RejectedExecutionException ex) {
                discardRunner.run();
            }
        }
    }

    @Override
    public void close() {
        synchronized (registeredHandles) {
            open = false;
        }
    }
}
