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

package org.apache.flink.changelog.fs;

import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.LocalRecoveryDirectoryProvider;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

import static org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorageAccess.CHECKPOINT_TASK_OWNED_STATE_DIR;

/** A StateChangeFsUploader implementation that writes the changes to remote and local. */
public class DuplicatingStateChangeFsUploader extends AbstractStateChangeFsUploader {

    private static final Logger LOG =
            LoggerFactory.getLogger(DuplicatingStateChangeFsUploader.class);

    private final Path basePath;
    private final FileSystem fileSystem;
    private final LocalRecoveryDirectoryProvider localRecoveryDirectoryProvider;

    public DuplicatingStateChangeFsUploader(
            Path basePath,
            FileSystem fileSystem,
            boolean compression,
            int bufferSize,
            ChangelogStorageMetricGroup metrics,
            TaskChangelogRegistry changelogRegistry,
            LocalRecoveryDirectoryProvider localRecoveryDirectoryProvider) {
        super(compression, bufferSize, metrics, changelogRegistry, FileStateHandle::new);
        this.basePath = basePath;
        this.fileSystem = fileSystem;
        this.localRecoveryDirectoryProvider = localRecoveryDirectoryProvider;
    }

    private Path getLocalDirectoryPath() {
        File outDir = localRecoveryDirectoryProvider.selectAllocationBaseDirectory(0);
        if (!outDir.exists() && !outDir.mkdirs()) {
            LOG.error(
                    "Local state base directory does not exist and could not be created: "
                            + outDir);
        }
        return new Path(outDir.toURI().toString(), CHECKPOINT_TASK_OWNED_STATE_DIR);
    }

    @Override
    public OutputStreamWithPos prepareStream() throws IOException {
        final String fileName = generateFileName();
        LOG.debug("upload tasks to {}", fileName);
        Path path = new Path(basePath, fileName);
        FSDataOutputStream primaryStream = fileSystem.create(path, WriteMode.NO_OVERWRITE);
        Path localPath = new Path(getLocalDirectoryPath(), fileName);
        FSDataOutputStream secondaryStream =
                localPath.getFileSystem().create(localPath, WriteMode.NO_OVERWRITE);
        DuplicatingOutputStreamWithPos outputStream =
                new DuplicatingOutputStreamWithPos(primaryStream, path, secondaryStream, localPath);
        outputStream.wrap(this.compression, this.bufferSize);
        return outputStream;
    }

    @Override
    public void close() throws Exception {}
}
