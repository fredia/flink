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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;
import static org.apache.flink.api.java.utils.ParameterTool.fromArgs;
import static org.apache.flink.streaming.api.datastream.DataStreamUtils.reinterpretAsKeyedStream;

/** A program useful in testing incremental checkpointing. */
public class IncrementalCheckpointsTestProgram {
    private static final Logger LOG =
            LoggerFactory.getLogger(IncrementalCheckpointsTestProgram.class);

    private static final String WORKLOAD_VALUE_STATE = "value-state";
    private static final String WORKLOAD_WINDOW = "window";

    public static void main(String[] args) throws Exception {
        ParameterTool params = fromArgs(args);

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(
                        IncrementalCheckpointsTestProgramConfigurator.conf(params));

        KeySelector<Long, Long> keySelector =
                getKeySelector(
                        // 10M of keys in total => 10Gb total state size with ~1K payload
                        // Also controls how incremental the updates are:
                        // when small, more updates made for the same keys during a checkpoint,
                        // making changelog less efficient forget the event after 10 occurrences.
                        params.getLong("keySpaceSize", 10_000_000L));

        DataStream<TestEvent> eventSource =
                addEventSource(
                        env,
                        // ~1K random bytes per event
                        params.getInt("payloadSizeBytes", 1000),
                        // with DoP = 20, 10K events per second in total
                        params.getInt("eventsPerSecondPerReader", 500),
                        keySelector);

        addWorkloads(eventSource, params, keySelector).addSink(new DiscardingSink<>());

        env.execute();
    }

    private static KeySelector<Long, Long> getKeySelector(long keySpaceSize) {
        return num -> num % keySpaceSize;
    }

    private static DataStream<TestEvent> addEventSource(
            StreamExecutionEnvironment env,
            int eventsPerSecondPerReader,
            int payloadSize,
            KeySelector<Long, Long> keySelector) {
        LOG.info("addEventSource, rate: {}, size: {}", eventsPerSecondPerReader, payloadSize);
        return env.fromSource(
                        new ThrottlingNumberSequenceSource(
                                0, Long.MAX_VALUE, eventsPerSecondPerReader),
                        WatermarkStrategy.noWatermarks(),
                        "Sequence Source")
                .map(keySelector::getKey)
                .returns(Long.class)
                .keyBy(keySelector)
                .map(
                        el -> {
                            // Thread.sleep(100); // don't block barriers
                            byte[] bytes = new byte[payloadSize];
                            ThreadLocalRandom.current().nextBytes(bytes);
                            return new TestEvent(el, bytes);
                        })
                .returns(TestEvent.class);
    }

    private static DataStream<TestEvent> addWorkloads(
            DataStream<TestEvent> stream,
            ParameterTool params,
            KeySelector<Long, Long> keySelector) {
        for (String workload : parseWorkloads(params.get("workloads", WORKLOAD_VALUE_STATE))) {
            switch (workload) {
                case WORKLOAD_VALUE_STATE:
                    stream =
                            addValueStateWorkload(
                                    reinterpretAsKeyedStream(stream, e -> keySelector.getKey(e.id)),
                                    params);
                    break;
                case WORKLOAD_WINDOW:
                    stream =
                            addWindowWorkload(
                                    reinterpretAsKeyedStream(stream, e -> keySelector.getKey(e.id)),
                                    Time.milliseconds(params.getInt("windowSize", 1000)),
                                    Time.milliseconds(params.getInt("windowSlide", 100)));
                    break;
                default:
                    throw new IllegalArgumentException("unknown workload: " + workload);
            }
        }
        return stream;
    }

    private static DataStream<TestEvent> addWindowWorkload(
            KeyedStream<TestEvent, Long> stream, Time windowSize, Time windowSlide) {
        LOG.info("addWindowWorkload, windowSize: {}, windowSlide:{}", windowSize, windowSlide);
        return stream.window(SlidingProcessingTimeWindows.of(windowSize, windowSlide))
                .process(
                        new ProcessWindowFunction<TestEvent, TestEvent, Long, TimeWindow>() {
                            @Override
                            public void process(
                                    Long key,
                                    ProcessWindowFunction<TestEvent, TestEvent, Long, TimeWindow>
                                                    .Context
                                            context,
                                    Iterable<TestEvent> elements,
                                    Collector<TestEvent> out) {}
                        });
    }

    private static DataStream<TestEvent> addValueStateWorkload(
            KeyedStream<TestEvent, Long> src, ParameterTool params) {
        int maxCountPerEvent = params.getInt("maxCountPerEvent", 10);
        LOG.info("addValueStateWorkload, maxCountPerEvent: {}", maxCountPerEvent);
        return src.map(new TestEventTestEventRichMapFunction(maxCountPerEvent));
    }

    private static final class TestEvent implements Serializable {
        private final long id;

        @SuppressWarnings({"FieldCanBeLocal", "unused"})
        private final byte[] payload;

        private TestEvent(long id, byte[] payload) {
            this.id = id;
            this.payload = payload;
        }
    }

    private static class TestEventTestEventRichMapFunction
            extends RichMapFunction<TestEvent, TestEvent> {

        private static final long serialVersionUID = 1L;

        private final int maxCountPerEvent;
        private transient ValueState<Integer> countState;
        private transient ValueState<TestEvent> lastEventState;

        private TestEventTestEventRichMapFunction(int maxCountPerEvent) {
            this.maxCountPerEvent = maxCountPerEvent;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            countState =
                    getRuntimeContext()
                            .getState(new ValueStateDescriptor<>("count", Integer.class));
            lastEventState =
                    getRuntimeContext()
                            .getState(new ValueStateDescriptor<>("lastEvent", TestEvent.class));
        }

        @Override
        public TestEvent map(TestEvent ev) throws Exception {
            Integer count = countState.value();
            count = count == null ? 0 : count;
            count++;
            if (count <= maxCountPerEvent) {
                countState.update(count);
                lastEventState.update(ev);
            } else {
                countState.clear();
                lastEventState.clear();
            }
            return ev;
        }
    }

    private static List<String> parseWorkloads(String workloads1) {
        return stream(workloads1.split("\\s*,\\s*")).map(String::toLowerCase).collect(toList());
    }
}
