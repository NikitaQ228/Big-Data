/*
 * Copyright 2017 data Artisans GmbH, 2019 Ververica GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.flinktraining.exercises.datastream_java.process;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.ververica.flinktraining.exercises.datastream_java.utils.MissingSolutionException;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.naming.Context;
import java.io.IOException;

/**
 * The "Expiring State" exercise from the Flink training
 * (http://training.ververica.com).
 *
 * The goal for this exercise is to enrich TaxiRides with fare information.
 *
 * Parameters:
 * -rides path-to-input-file
 * -fares path-to-input-file
 *
 */
public class ExpiringStateExercise extends ExerciseBase {
	static final OutputTag<TaxiRide> unmatchedRides = new OutputTag<TaxiRide>("unmatchedRides") {};
	static final OutputTag<TaxiFare> unmatchedFares = new OutputTag<TaxiFare>("unmatchedFares") {};

	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		final String ridesFile = params.get("rides", ExerciseBase.pathToRideData);
		final String faresFile = params.get("fares", ExerciseBase.pathToFareData);

		final int maxEventDelay = 60;           // events are out of order by max 60 seconds
		final int servingSpeedFactor = 600; 	// 10 minutes worth of events are served every second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(ExerciseBase.parallelism);

		DataStream<TaxiRide> rides = env
				.addSource(rideSourceOrTest(new TaxiRideSource(ridesFile, maxEventDelay, servingSpeedFactor)))
				.filter((TaxiRide ride) -> (ride.isStart && (ride.rideId % 1000 != 0)))
				.keyBy(ride -> ride.rideId);

		DataStream<TaxiFare> fares = env
				.addSource(fareSourceOrTest(new TaxiFareSource(faresFile, maxEventDelay, servingSpeedFactor)))
				.keyBy(fare -> fare.rideId);

		SingleOutputStreamOperator processed = rides
				.connect(fares)
				.process(new EnrichmentFunction());

		printOrTest(processed.getSideOutput(unmatchedFares));

		env.execute("ExpiringStateExercise (java)");
	}

	public static class EnrichmentFunction extends KeyedCoProcessFunction<Long, TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {

		private static final long TIMEOUT_MS = 2 * 60 * 60 * 1000L;

		private ValueState<TaxiRide> rideState;
		private ValueState<TaxiFare> fareState;
		private ValueState<Long> timerState;

		@Override
		public void open(Configuration config) {
			rideState = getRuntimeContext().getState(
					new ValueStateDescriptor<>("ride-state", TaxiRide.class));
			fareState = getRuntimeContext().getState(
					new ValueStateDescriptor<>("fare-state", TaxiFare.class));
			timerState = getRuntimeContext().getState(
					new ValueStateDescriptor<>("timer-state", Types.LONG));
		}

		@Override
		public void processElement1(TaxiRide ride, Context ctx,
									Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {

			TaxiFare fare = fareState.value();

			if (fare != null) {
				out.collect(Tuple2.of(ride, fare));
				cleanup();
			} else {
				rideState.update(ride);
				registerTimer(ctx, ride.startTime.getMillis());
			}
		}

		@Override
		public void processElement2(TaxiFare fare, Context ctx,
									Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {

			TaxiRide ride = rideState.value();

			if (ride != null) {
				out.collect(Tuple2.of(ride, fare));
				cleanup();
			} else {
				fareState.update(fare);
				registerTimer(ctx, fare.startTime.getMillis());
			}
		}

		@Override
		public void onTimer(long timestamp, OnTimerContext ctx,
							Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {

			TaxiRide ride = rideState.value();
			TaxiFare fare = fareState.value();

			if (ride != null) {
				ctx.output(unmatchedRides, ride);
			}
			if (fare != null) {
				ctx.output(unmatchedFares, fare);
			}

			// Очищаем состояние в любом случае
			cleanup();
		}

		/** Регистрирует таймер на (время события + 2 часа), если ещё не зарегистрирован */
		private void registerTimer(Context ctx, long eventTime) throws Exception {
			Long currentTimer = timerState.value();
			long newTimer = eventTime + TIMEOUT_MS;

			if (currentTimer == null || newTimer < currentTimer) {
				ctx.timerService().registerEventTimeTimer(newTimer);
				timerState.update(newTimer);
			}
		}

		/** Очищает все состояния */
		private void cleanup() throws IOException {
			rideState.clear();
			fareState.clear();
			timerState.clear();
		}
	}
}