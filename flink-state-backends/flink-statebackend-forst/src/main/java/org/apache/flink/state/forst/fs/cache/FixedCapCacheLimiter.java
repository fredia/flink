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

package org.apache.flink.state.forst.fs.cache;

/** A fixed capacity resource checker. */
public class FixedCapCacheLimiter implements CacheLimiter {
    private final long capacity;
    private long usageResource;

    public FixedCapCacheLimiter(long capacity) {
        this.capacity = capacity;
        this.usageResource = 0;
    }

    @Override
    public boolean isOverflow(long toAddSize) {
        return usageResource + toAddSize > capacity;
    }

    @Override
    public boolean acquire(long toAddSize) {
        if (isOverflow(toAddSize)) {
            return false;
        }
        usageResource += toAddSize;
        return true;
    }

    @Override
    public void release(long toReleaseSize) {
        usageResource -= Math.min(usageResource, toReleaseSize);
    }
}
