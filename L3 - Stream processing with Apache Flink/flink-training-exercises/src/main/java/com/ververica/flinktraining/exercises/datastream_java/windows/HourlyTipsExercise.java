/*
 * Copyright 2018 data Artisans GmbH, 2019 Ververica GmbH
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

package com.ververica.flinktraining.exercises.datastream_java.windows;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.io.IOException;

/**
 * The "Hourly Tips" exercise of the Flink training
 * (http://training.ververica.com).
 *
 * The task of the exercise is to first calculate the total tips collected by each driver, hour by hour, and
 * then from that stream, find the highest tip total in each hour.
 *
 * Parameters:
 * -input path-to-input-file
 *
 */
public class HourlyTipsExercise extends ExerciseBase {

	public static void main(String[] args) throws Exception {

		// read parameters
		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = params.get("input", ExerciseBase.pathToFareData);

		final int maxEventDelay = 60;       // events are out of order by max 60 seconds
		final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(ExerciseBase.parallelism);

		// start the data generator
		DataStream<TaxiFare> fares = env.addSource(fareSourceOrTest(new TaxiFareSource(input, maxEventDelay, servingSpeedFactor)));

		// Сумма чаевых по (водитель, час)
		DataStream<Tuple3<Long, Long, Float>> hourlyTips = fares
				.assignTimestampsAndWatermarks(new FareTimestampExtractor())
				.keyBy(fare -> fare.driverId)
				.timeWindow(Time.hours(1))
				.process(new ProcessWindowFunction<TaxiFare, Tuple3<Long, Long, Float>, Long, TimeWindow>() {
					@Override
					public void process(Long driverId, Context ctx, Iterable<TaxiFare> elements, Collector<Tuple3<Long, Long, Float>> out) {
						float sum = 0f;
						for (TaxiFare f : elements) sum += f.tip;
						out.collect(Tuple3.of(ctx.window().getEnd(), driverId, sum));
					}
				});

		// Находим финальный максимум по каждому часу
		DataStream<Tuple3<Long, Long, Float>> hourlyMax = hourlyTips
				.keyBy(t -> t.f0) // ключ = окончание часа
				.process(new MaxTipPerHourFunction());

		printOrTest(hourlyMax);
		env.execute("Hourly Tips (java)");
	}

	/**
	 * Функция, которая накапливает максимум чаевых для каждого часа
	 * и эмитит результат ровно один раз при срабатывании таймера.
	 */
	public static class MaxTipPerHourFunction
			extends KeyedProcessFunction<Long, Tuple3<Long, Long, Float>, Tuple3<Long, Long, Float>> {

		private ValueState<Tuple2<Long, Float>> maxState;
		private ValueState<Boolean> timerRegistered;

		@Override
		public void open(Configuration config) {
			maxState = getRuntimeContext().getState(
					new ValueStateDescriptor<>("maxTip", Types.TUPLE(Types.LONG, Types.FLOAT)));
			timerRegistered = getRuntimeContext().getState(
					new ValueStateDescriptor<>("timerReg", Types.BOOLEAN));
		}

		@Override
		public void processElement(Tuple3<Long, Long, Float> value,
								   Context ctx, Collector<Tuple3<Long, Long, Float>> out) throws IOException {

			// Обновляем максимум, если текущие чаевые больше сохранённых
			Tuple2<Long, Float> currentMax = maxState.value();
			if (currentMax == null || value.f2 > currentMax.f1) {
				maxState.update(Tuple2.of(value.f1, value.f2));
			}

			// Регистрируем таймер один раз на конец часа
			if (!Boolean.TRUE.equals(timerRegistered.value())) {
				// Таймер сработает, когда watermark достигнет value.f0
				ctx.timerService().registerEventTimeTimer(value.f0);
				timerRegistered.update(true);
			}
		}

		@Override
		public void onTimer(long timestamp, OnTimerContext ctx,
							Collector<Tuple3<Long, Long, Float>> out) throws IOException {
			Tuple2<Long, Float> result = maxState.value();
			if (result != null) {
				// Эмитим финальный максимум для этого часа
				out.collect(Tuple3.of(timestamp, result.f0, result.f1));
				// Очищаем состояние, чтобы не мешать следующим окнам
				maxState.clear();
				timerRegistered.clear();
			}
		}
	}

	// Генератор временных меток и водяных знаков
	public static class FareTimestampExtractor implements AssignerWithPeriodicWatermarks<TaxiFare> {
		private long maxTimestamp = Long.MIN_VALUE;

		@Override
		public long extractTimestamp(TaxiFare fare, long previousTimestamp) {
			long ts = fare.startTime.getMillis();
			maxTimestamp = Math.max(maxTimestamp, ts);
			return ts;
		}

		@Nullable
		@Override
		public Watermark getCurrentWatermark() {
			return new Watermark(maxTimestamp);
		}
	}
}