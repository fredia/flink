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

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.asyncprocessing.ReferenceCounted;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * A file cache entry that encapsulates file and the size of the file, and provides methods to read
 * or write file. Not thread safe.
 */
public class FileCacheEntry extends ReferenceCounted {
    private static final Logger LOG = LoggerFactory.getLogger(FileCacheEntry.class);

    /** The reference of file cache. */
    final FileCache fileCache;

    /** The file system of cache. */
    final FileSystem cacheFs;

    /** The original path of file. */
    final Path originalPath;

    /** The path in cache. */
    final Path cachePath;

    /** The size of file. */
    long entrySize;

    volatile FSDataInputStream fsDataInputStream;

    volatile FSDataOutputStream fsDataOutputStream;

    /** Whether the entry is writing. */
    volatile boolean writing;

    volatile boolean closed;

    FileCacheEntry(FileCache fileCache, Path originalPath, Path cachePath) {
        super(1);
        this.fileCache = fileCache;
        this.cacheFs = fileCache.cacheFs;
        this.originalPath = originalPath;
        this.cachePath = cachePath;
        this.entrySize = 0;
        this.writing = true;
        this.closed = false;
    }

    FSDataInputStream open4Read() throws IOException {
        if (!closed && !writing && tryRetain() > 0) {
            if (fsDataInputStream == null) {
                fsDataInputStream = cacheFs.open(cachePath);
            }
            return fsDataInputStream;
        }
        return null;
    }

    public void close4Read() {
        release();
    }

    FSDataOutputStream open4Write() throws IOException {
        if (!closed && writing && tryRetain() > 0) {
            if (fsDataOutputStream == null) {
                fsDataOutputStream = cacheFs.create(cachePath, FileSystem.WriteMode.OVERWRITE);
            }
            return fsDataOutputStream;
        }
        return null;
    }

    public void close4Write() throws IOException {
        long thisSize = fsDataOutputStream.getPos();
        fsDataOutputStream.close();
        entrySize += thisSize;
        fsDataOutputStream = null;
        writing = false;
        release();
        fileCache.put(cachePath.toString(), this);
    }

    public void invalidate() {
        if (!closed) {
            closed = true;
            release();
        }
    }

    @Override
    protected void referenceCountReachedZero(@Nullable Object o) {
        try {
            if (fsDataInputStream != null) {
                fsDataInputStream.close();
                fsDataInputStream = null;
            }
            cacheFs.delete(cachePath, false);
        } catch (Exception e) {
            LOG.warn("Failed to delete cache entry {}.", cachePath, e);
        }
    }
}
