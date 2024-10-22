/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

package org.apache.flink.streaming.examples.wordcount3;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ParameterTool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;

import static org.apache.flink.streaming.examples.wordcount2.JobConfig.FLAT_MAP_PARALLELISM;
import static org.apache.flink.streaming.examples.wordcount2.JobConfig.JOB_NAME;
import static org.apache.flink.streaming.examples.wordcount2.JobConfig.SHARING_GROUP;
import static org.apache.flink.streaming.examples.wordcount2.JobConfig.STATE_MODE;
import static org.apache.flink.streaming.examples.wordcount2.JobConfig.StateMode;
import static org.apache.flink.streaming.examples.wordcount2.JobConfig.TTL;
import static org.apache.flink.streaming.examples.wordcount2.JobConfig.WORD_LENGTH;
import static org.apache.flink.streaming.examples.wordcount2.JobConfig.WORD_NUMBER;
import static org.apache.flink.streaming.examples.wordcount2.JobConfig.WORD_RATE;
import static org.apache.flink.streaming.examples.wordcount2.JobConfig.configureCheckpoint;
import static org.apache.flink.streaming.examples.wordcount2.JobConfig.getConfiguration;
import static org.apache.flink.streaming.examples.wordcount2.JobConfig.setStateBackend;

/**
 * Benchmark mainly used for {@link ValueState} and only support 1 parallelism.
 */
public class WordCount {

	private static final Logger LOG = LoggerFactory.getLogger(WordCount.class);

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool params = ParameterTool.fromArgs(args);
        Configuration configuration = getConfiguration(params);
        //configRemoteStateBackend(configuration);
		env.getConfig().setGlobalJobParameters(configuration);
		env.disableOperatorChaining();

		String jobName = configuration.get(JOB_NAME);

		configureCheckpoint(env, configuration);

		String group1 = "default1";
		String group2 = "default2";
		String group3 = "default3";
		if (configuration.get(SHARING_GROUP)) {
			group1 = group2 = group3 = "default";
		}

		setStateBackend(env, configuration);

		// configure source
		int wordNumber = configuration.get(WORD_NUMBER);
		int wordLength = configuration.get(WORD_LENGTH);
		int wordRate = configuration.get(WORD_RATE);

		DataStream<Tuple2<String, Long>> source =
				WordSource.getSource(env, wordRate, wordNumber, wordLength).setParallelism(1)
                        .slotSharingGroup(group1);

		// configure ttl
		long ttl = configuration.get(TTL).toMillis();

		FlatMapFunction<Tuple2<String, Long>, Long> flatMapFunction =
			getFlatMapFunction(configuration, ttl);
		DataStream<Long> mapper = source.keyBy(e->e.f0)
				.flatMap(flatMapFunction)
				.setParallelism(configuration.get(FLAT_MAP_PARALLELISM))
                .slotSharingGroup(group2);

		//mapper.print().setParallelism(1);
        mapper.addSink(new BlackholeSink<>()).slotSharingGroup(group3).setParallelism(1);

		if (jobName == null) {
			env.execute();
		} else {
			env.execute(jobName);
		}
	}

	private static FlatMapFunction<Tuple2<String, Long>, Long> getFlatMapFunction(Configuration configuration, long ttl) {
		StateMode stateMode =
			StateMode.valueOf(configuration.get(STATE_MODE).toUpperCase());

		switch (stateMode) {
			case MIXED:
			default:
				return new MixedFlatMapper(ttl);
		}
	}

	/**
	 * Write and read mixed mapper.
	 */
	public static class MixedFlatMapper extends RichFlatMapFunction<Tuple2<String, Long>, Long> {

		private transient ValueState<Integer> wordCounter;

		private final long ttl;

		public MixedFlatMapper(long ttl) {
			this.ttl = ttl;
		}

		@Override
		public void flatMap(Tuple2<String, Long> in, Collector<Long> out) throws IOException {
            Integer val = wordCounter.value();
            if (val!=null) {
                wordCounter.update(val + 1);
                out.collect(val + 1L);
            } else {
                wordCounter.update(1);
                out.collect(1L);
            }
		}

		@Override
		public void open(OpenContext context) {
            ValueStateDescriptor<Integer> descriptor =
                    new ValueStateDescriptor<>(
                            "wc",
                            TypeInformation.of(new TypeHint<Integer>(){}));
			if (ttl > 0) {
				LOG.info("Setting ttl to {}ms.", ttl);
				StateTtlConfig ttlConfig = StateTtlConfig
						.newBuilder(Duration.ofMillis(ttl))
						.setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
						.setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
						.build();
				descriptor.enableTimeToLive(ttlConfig);
			}
			wordCounter = getRuntimeContext().getState(descriptor);
		}
	}
}
