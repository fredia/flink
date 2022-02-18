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

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.RestOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import static org.apache.flink.changelog.fs.FsStateChangelogOptions.PREEMPTIVE_PERSIST_THRESHOLD;
import static org.apache.flink.configuration.CheckpointingOptions.CHECKPOINTS_DIRECTORY;
import static org.apache.flink.configuration.CheckpointingOptions.CHECKPOINT_STORAGE;
import static org.apache.flink.configuration.CheckpointingOptions.FS_SMALL_FILE_THRESHOLD;
import static org.apache.flink.configuration.CheckpointingOptions.INCREMENTAL_CHECKPOINTS;
import static org.apache.flink.configuration.CheckpointingOptions.LOCAL_RECOVERY;
import static org.apache.flink.configuration.JobManagerOptions.JOB_MANAGER_IO_POOL_SIZE;
import static org.apache.flink.configuration.PipelineOptions.OBJECT_REUSE;
import static org.apache.flink.configuration.RestOptions.ENABLE_FLAMEGRAPH;
import static org.apache.flink.configuration.RestartStrategyOptions.RESTART_STRATEGY;
import static org.apache.flink.configuration.StateBackendOptions.STATE_BACKEND;
import static org.apache.flink.configuration.StateChangelogOptions.ENABLE_STATE_CHANGE_LOG;
import static org.apache.flink.configuration.StateChangelogOptions.PERIODIC_MATERIALIZATION_INTERVAL;
import static org.apache.flink.configuration.StateChangelogOptions.STATE_CHANGE_LOG_STORAGE;
import static org.apache.flink.configuration.WebOptions.CHECKPOINTS_HISTORY_SIZE;
import static org.apache.flink.streaming.api.CheckpointingMode.EXACTLY_ONCE;
import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL;
import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.CHECKPOINTING_MODE;
import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.ENABLE_UNALIGNED;

class IncrementalCheckpointsTestProgramConfigurator {
    private static final Logger LOG =
            LoggerFactory.getLogger(IncrementalCheckpointsTestProgramConfigurator.class);

    public static Configuration conf(ParameterTool params) {
        Configuration conf = new Configuration();

        basicSetup(conf);
        configureBackPressure(conf);
        configureDebugging(conf);

        switch (params.get("preset")) {
            case "flip-151":
                configureFlip151(conf, params);
                break;
            case "flip-158":
                configureFlip158(conf, params);
                break;
            case "none":
                // explicitly require to specify no preset
                break;
            default:
                throw new IllegalArgumentException("unknown scenario: " + params.getInt("preset"));
        }

        // override if needed
        conf.addAll(params.getConfiguration());

        LOG.info("built config: {}", conf);
        return conf;
    }

    private static void basicSetup(Configuration conf) {
        conf.set(CoreOptions.DEFAULT_PARALLELISM, 20);
        conf.set(CHECKPOINTING_MODE, EXACTLY_ONCE);
        conf.set(CHECKPOINTING_INTERVAL, Duration.ofMillis(10)); // as fast as can
        conf.set(CHECKPOINT_STORAGE, "filesystem");
        // do NOT override cluster value
        conf.set(
                CHECKPOINTS_DIRECTORY,
                "s3://changelog-backend-testing-eu/changelog"); // TODO: remove
        conf.set(FS_SMALL_FILE_THRESHOLD, MemorySize.ofMebiBytes(1));
    }

    private static void configureBackPressure(Configuration conf) {
        // tried: UC, BD, smaller network size - didn't work (UC helps a bit)
        // non-blocking throttling in sources did work
        conf.set(ENABLE_UNALIGNED, true);
        // conf.set(BUFFER_DEBLOAT_ENABLED, true);
        // conf.set(BUFFER_DEBLOAT_TARGET, Duration.ofMillis(100));
        // conf.set(BUFFER_DEBLOAT_PERIOD, Duration.ofMillis(50));
        // conf.set(BUFFER_DEBLOAT_SAMPLES, 10);
        // conf.set(NETWORK_MEMORY_MAX, MemorySize.ofMebiBytes(100));
    }

    private static void configureFlip151(Configuration conf, ParameterTool params) {
        conf.set(
                STATE_BACKEND,
                params.getBoolean(INCREMENTAL_CHECKPOINTS.key())
                        ? "org.apache.flink.state.hashmap.IncrementalHashMapStateBackendFactory"
                        : "org.apache.flink.runtime.state.hashmap.HashMapStateBackendFactory");
        // todo: tune memory?
        conf.set(OBJECT_REUSE, false); // not supported
        conf.set(LOCAL_RECOVERY, false); // not supported
        // cluster-level (for reference only)
        conf.set(JOB_MANAGER_IO_POOL_SIZE, 50); // FLINK-26590
    }

    private static void configureFlip158(Configuration conf, ParameterTool params) {
        conf.set(ENABLE_STATE_CHANGE_LOG, params.getBoolean(ENABLE_STATE_CHANGE_LOG.key()));
        conf.set(
                STATE_BACKEND,
                "org.apache.flink.contrib.streaming.state.RocksDBStateBackendFactory");
        conf.set(INCREMENTAL_CHECKPOINTS, true);
        conf.set(PREEMPTIVE_PERSIST_THRESHOLD, MemorySize.ofMebiBytes(10));
        conf.set(PERIODIC_MATERIALIZATION_INTERVAL, Duration.ofMinutes(3));
        conf.set(LOCAL_RECOVERY, false); // not supported
        // cluster-level (for reference only)
        conf.set(STATE_CHANGE_LOG_STORAGE, "filesystem");
        conf.set(JOB_MANAGER_IO_POOL_SIZE, 250); // FLINK-26590
    }

    private static void configureDebugging(Configuration conf) {
        conf.set(RestOptions.PORT, 8081);
        conf.set(RESTART_STRATEGY, "fixed-delay");
        // cluster-level (for reference only)
        conf.set(CHECKPOINTS_HISTORY_SIZE, 100);
        conf.set(ENABLE_FLAMEGRAPH, true);
    }
}
