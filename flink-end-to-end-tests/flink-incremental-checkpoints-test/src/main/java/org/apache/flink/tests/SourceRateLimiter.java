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

package org.apache.flink.tests;

import org.apache.flink.annotation.Internal;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Simple token bucket implementation for single-threaded access. */
@Internal
final class SourceRateLimiter {
    private final AtomicBoolean newTokensAdded = new AtomicBoolean(false);
    private final int tokensToAdd;
    private int tokensAvailable;

    public SourceRateLimiter(int tokensPerSecond) {
        this(
                tokensPerSecond < 10 ? 1000 : 100,
                tokensPerSecond < 10 ? tokensPerSecond : tokensPerSecond / 10);
    }

    public SourceRateLimiter(int intervalMs, int tokensToAdd) {
        checkArgument(intervalMs > 0);
        checkArgument(tokensToAdd > 0);
        this.tokensToAdd = tokensToAdd;
        this.tokensAvailable = tokensToAdd;
        new Timer("source-limiter", true)
                .scheduleAtFixedRate(
                        new TimerTask() {
                            @Override
                            public void run() {
                                newTokensAdded.set(true); // "catch up" is ok
                            }
                        },
                        intervalMs,
                        intervalMs);
    }

    public boolean request() {
        if (tokensAvailable == 0 && newTokensAdded.compareAndSet(true, false)) {
            tokensAvailable = tokensToAdd;
        }
        if (tokensAvailable > 0) {
            tokensAvailable--;
            return true;
        } else {
            return false;
        }
    }
}
