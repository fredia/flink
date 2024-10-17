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

import org.apache.flink.core.fs.ByteBufferReadable;
import org.apache.flink.core.fs.FSDataInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A {@link FSDataInputStream} delegates requests to other one and supports reading data with {@link
 * ByteBuffer}.
 */
public class CachedDataInputStream extends FSDataInputStream implements ByteBufferReadable {

    private static final Logger LOG = LoggerFactory.getLogger(CachedDataInputStream.class);

    private final FileCacheEntry cacheEntry;

    private final Object lock;

    private volatile FSDataInputStream fsdis;

    public CachedDataInputStream(FileCacheEntry cacheEntry) {
        this.cacheEntry = cacheEntry;
        this.lock = new Object();
    }

    private FSDataInputStream getStream() throws IOException {
        if (fsdis == null) {
            synchronized (lock) {
                if (fsdis == null) {
                    fsdis = cacheEntry.openForRead();
                }
            }
        }
        return fsdis;
    }

    private void closeStream() {
        synchronized (lock) {
            if (fsdis != null) {
                cacheEntry.closeForRead();
                fsdis = null;
            }
        }
    }

    public boolean isAvailable() throws IOException {
        return getStream() != null;
    }

    @Override
    public void seek(long desired) throws IOException {
        getStream().seek(desired);
    }

    @Override
    public long getPos() throws IOException {
        return getStream().getPos();
    }

    @Override
    public int read() throws IOException {
        return getStream().read();
    }

    @Override
    public int read(byte[] b) throws IOException {
        return getStream().read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return getStream().read(b, off, len);
    }

    @Override
    public long skip(long n) throws IOException {
        return getStream().skip(n);
    }

    @Override
    public int available() throws IOException {
        return getStream().available();
    }

    @Override
    public void close() throws IOException {
        closeStream();
    }

    @Override
    public synchronized void mark(int readlimit) {
        try {
            getStream().mark(readlimit);
        } catch (Exception e) {
            LOG.warn("Mark error.", e);
        }
    }

    @Override
    public synchronized void reset() throws IOException {
        getStream().reset();
    }

    @Override
    public boolean markSupported() {
        try {
            return getStream().markSupported();
        } catch (Exception e) {
            LOG.warn("MarkSupported error.", e);
        }
        return false;
    }

    @Override
    public int read(ByteBuffer bb) throws IOException {
        byte[] tmp = new byte[bb.remaining()];
        int n = 0;
        while (n < tmp.length) {
            int read = getStream().read(tmp, n, tmp.length - n);
            if (read == -1) {
                break;
            }
            n += read;
        }
        if (n > 0) {
            bb.put(tmp, 0, n);
        }
        closeStream();
        return n;
    }

    @Override
    public int read(long position, ByteBuffer bb) throws IOException {
        getStream().seek(position);
        return read(bb);
    }
}
