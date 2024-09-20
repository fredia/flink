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

package org.apache.flink.runtime.asyncprocessing;

import org.apache.flink.util.Preconditions;

import java.util.concurrent.CompletableFuture;

/** The mocked {@link StateExecutor} for testing. */
public class MockStateExecutor implements StateExecutor {

    @Override
    public CompletableFuture<Void> executeBatchRequests(
            StateRequestContainer stateRequestContainer) {
        Preconditions.checkArgument(stateRequestContainer instanceof MockStateRequestContainer);
        for (StateRequest<?, ?, ?> request :
                ((MockStateRequestContainer) stateRequestContainer).getStateRequestList()) {
            request.getFuture().complete(null);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public StateRequestContainer createStateRequestContainer() {
        return new MockStateRequestContainer();
    }

    @Override
    public long ongoingRequests() {
        return 0;
    }

    @Override
    public void shutdown() {}
}
