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

package org.apache.flink.state.forst;

import org.apache.flink.api.common.eventtime.BoundedOutOfOrdernessWatermarks;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.state.v2.ValueStateDescriptor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.util.Collector;

import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Random;

import static org.apache.flink.configuration.CheckpointingOptions.CHECKPOINTS_DIRECTORY;
import static org.apache.flink.configuration.CheckpointingOptions.INCREMENTAL_CHECKPOINTS;

public class WordCountTest {

    //    @ClassRule
    //    public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE =
    //            new MiniClusterWithClientResource(
    //                    new MiniClusterResourceConfiguration.Builder()
    //                            .setConfiguration(getCommonConfiguration())
    //                            .setNumberTaskManagers(1)
    //                            .setNumberSlotsPerTaskManager(1)
    //                            .build());

    private static Configuration getCommonConfiguration() {
        Configuration config = new Configuration();
        config.set(INCREMENTAL_CHECKPOINTS, true);

        config.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("128m"));
        config.set(
                StateBackendOptions.STATE_BACKEND,
                "org.apache.flink.state.forst.ForStStateBackendFactory");
        return config;
    }

    @Test
    public void testWordCountWithLocal() throws Exception {
        Configuration config = getCommonConfiguration();
        FileSystem.initialize(config, null);
        config.set(CHECKPOINTS_DIRECTORY, "file:///tmp/checkpoint");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        // env.enableCheckpointing(500);
        DataStream<String> source = WordSource.getSource(env, 1000000, 3, 50).setParallelism(1);
        DataStream<Long> mapper =
                source.keyBy(e -> e).flatMap(new MixedFlatMapper()).setParallelism(1);
        mapper.print().setParallelism(1);
        env.execute();
    }

    @Test
    public void syncExeWordCountWithASyncAPI() throws Exception {
        Configuration config = getCommonConfiguration();
        config.set(StateBackendOptions.STATE_BACKEND, "hashmap");
        FileSystem.initialize(config, null);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.enableCheckpointing(500);
        DataStream<String> source = WordSource.getSource(env, 1000, 10, 50).setParallelism(1);
        DataStream<Long> mapper =
                source.keyBy(e -> e).flatMap(new MixedFlatMapper()).setParallelism(1);
        mapper.print().setParallelism(1);
        env.execute();
    }

    private static class IngestionTimeWatermarkStrategy<T> implements WatermarkStrategy<T> {

        private Random random = new Random();

        private IngestionTimeWatermarkStrategy() {}

        public static <T> IngestionTimeWatermarkStrategy<T> create() {
            return new IngestionTimeWatermarkStrategy<>();
        }

        @Override
        public WatermarkGenerator<T> createWatermarkGenerator(
                WatermarkGeneratorSupplier.Context context) {
            return new BoundedOutOfOrdernessWatermarks<>(Duration.ofMillis(100));
        }

        @Override
        public TimestampAssigner<T> createTimestampAssigner(
                TimestampAssignerSupplier.Context context) {
            long delay = random.nextInt(10);
            return (event, timestamp) -> System.currentTimeMillis() - delay;
        }
    }

    public static class MixedFlatMapper extends RichFlatMapFunction<String, Long> {

        private transient ValueState<Integer> asyncWordCounter;

        public MixedFlatMapper() {}

        @Override
        public void flatMap(String in, Collector<Long> out) throws IOException {
            asyncWordCounter
                    .asyncValue()
                    .thenAccept(
                            currentValue -> {
                                if (currentValue != null) {
                                    asyncWordCounter
                                            .asyncUpdate(currentValue + 1)
                                            .thenAccept(
                                                    empty -> {
                                                        out.collect(currentValue + 1L);
                                                    });
                                } else {
                                    asyncWordCounter
                                            .asyncUpdate(1)
                                            .thenAccept(
                                                    empty -> {
                                                        out.collect(1L);
                                                    });
                                }
                            });
        }

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<Integer> descriptor =
                    new ValueStateDescriptor<>(
                            "wc", TypeInformation.of(new TypeHint<Integer>() {}));
            asyncWordCounter =
                    ((StreamingRuntimeContext) getRuntimeContext()).getValueState(descriptor);
        }
    }
}
