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

package org.apache.flink.training.exercises.hourlytips;

import com.sun.org.apache.bcel.internal.generic.FNEG;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;
import org.apache.flink.training.exercises.common.sources.TaxiFareGenerator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * The Hourly Tips exercise from the Flink training.
 *
 * <p>The task of the exercise is to first calculate the total tips collected by each driver, hour
 * by hour, and then from that stream, find the highest tip total in each hour.
 */
public class HourlyTipsExercise {

  private static final OutputTag<TaxiFare> lateFares = new OutputTag<>("lateFares");
  private final SourceFunction<TaxiFare> source;
  private final SinkFunction<Tuple3<Long, Long, Float>> sink;

  /**
   * Creates a job using the source and sink provided.
   */
  public HourlyTipsExercise(
      SourceFunction<TaxiFare> source, SinkFunction<Tuple3<Long, Long, Float>> sink) {

    this.source = source;
    this.sink = sink;
  }

  /**
   * Main method.
   *
   * @throws Exception which occurs during job execution.
   */
  public static void main(String[] args) throws Exception {

    HourlyTipsExercise job =
        new HourlyTipsExercise(new TaxiFareGenerator(), new PrintSinkFunction<>());

//    job.execute();
    job.execute2();
  }

  /**
   * Create and execute the hourly tips pipeline.
   *
   * @return {JobExecutionResult}
   * @throws Exception which occurs during job execution.
   */
  public JobExecutionResult execute() throws Exception {

    // set up streaming execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // start the data generator and arrange for watermarking
    DataStream<TaxiFare> fares = env.addSource(source)
        .assignTimestampsAndWatermarks(
            WatermarkStrategy.<TaxiFare>forMonotonousTimestamps().withTimestampAssigner(
                (element, recordTimestamp) -> element.getEventTimeMillis()));

    // compute tips per hour for each driver
    DataStream<Tuple3<Long, Long, Float>> hourlyTips = fares
        .keyBy(taxiFare -> taxiFare.driverId)
        .window(TumblingEventTimeWindows.of(Time.hours(1)))
        .process(new AddTips());

    // find the driver with the highest sum of tips for each hour
    DataStream<Tuple3<Long, Long, Float>> hourlyMax = hourlyTips.windowAll(
        TumblingEventTimeWindows.of(Time.hours(1))).maxBy(2);// 2是 Tuple3 的第三个元素

    hourlyMax.addSink(sink);

    // execute the pipeline and return the result
    return env.execute("Hourly Tips");
  }

  // 另外一个实现方式名：基于 KeyedProcessFunction
  public JobExecutionResult execute2() throws Exception {

    // set up streaming execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // start the data generator and arrange for watermarking
    DataStream<TaxiFare> fares = env.addSource(source)
        .assignTimestampsAndWatermarks(
            WatermarkStrategy.<TaxiFare>forMonotonousTimestamps().withTimestampAssigner(
                (element, recordTimestamp) -> element.getEventTimeMillis()));

    SingleOutputStreamOperator<Tuple3<Long, Long, Float>> hourlyTips = fares.keyBy(
            taxifare -> taxifare.driverId)
        .process(new PseudoWindow(Time.hours(1)));

    hourlyTips.addSink(sink);

    // 旁路输出(Side output)
    hourlyTips.getSideOutput(lateFares).print();

    // execute the pipeline and return the result
    return env.execute("Hourly Tips");
  }

  private static class AddTips extends
      ProcessWindowFunction<TaxiFare, Tuple3<Long, Long, Float>, Long, TimeWindow> {

    @Override
    public void process(Long key,
        Context context,
        Iterable<TaxiFare> fares,
        Collector<Tuple3<Long, Long, Float>> out) throws Exception {
      float sumOfTips = 0F;
      for (TaxiFare fare : fares) {
        sumOfTips += fare.tip;
      }
      out.collect(Tuple3.of(context.window().getEnd(), key, sumOfTips));
    }
  }

  private class PseudoWindow extends
      KeyedProcessFunction<Long, TaxiFare, Tuple3<Long, Long, Float>> {

    private final long durationMsec;

    // 每个窗口都持有托管的 Keyed state 的入口，并且根据窗口的结束时间执行 keyed 策略。
    // 每个司机都有一个单独的MapState对象。key-时间戳
    private transient MapState<Long, Float> sumOfTips;


    public PseudoWindow(Time duration) {
      this.durationMsec = duration.toMilliseconds();
    }

    // 在初始化期间调用一次。
    @Override
    public void open(Configuration parameters) throws Exception {
      MapStateDescriptor<Long, Float> sumOfTipsDesc = new MapStateDescriptor<>("sumOfTips",
          Long.class,
          Float.class);
      sumOfTips = getRuntimeContext().getMapState(sumOfTipsDesc);
    }

    // 每个票价事件（TaxiFare-Event）输入（到达）时调用，以处理输入的票价事件。
    @Override
    public void processElement(TaxiFare fare,
        KeyedProcessFunction<Long, TaxiFare, Tuple3<Long, Long, Float>>.Context ctx,
        Collector<Tuple3<Long, Long, Float>> out) throws Exception {
      long eventTime = fare.getEventTimeMillis();
      TimerService timerService = ctx.timerService();

      if (eventTime <= timerService.currentWatermark()) {
        // 事件延迟；其对应的窗口已经触发。
        ctx.output(lateFares, fare);
      } else {
        // 将 eventTime 向上取值并将结果赋值到包含当前事件的窗口的末尾时间点。
        long endOfWindow = (eventTime - (eventTime % durationMsec) + durationMsec - 1);

        // 在窗口完成时将启用回调
        timerService.registerEventTimeTimer(endOfWindow);

        // 将此票价的小费添加到该窗口的总计中
        Float sum = sumOfTips.get(endOfWindow);
        if (sum == null) {
          sum = 0F;
        } else {
          sum += fare.tip;
          sumOfTips.put(endOfWindow, sum);
        }
      }
    }

    // 当当前水印（watermark）表明窗口现在需要完成的时候调用。
    //
    // 传递给 onTimer 的 OnTimerContext context 可用于确定当前 key。
    // 我们的 pseudo-windows 在当前 Watermark 到达每小时结束时触发，此时调用 onTimer。
    //这个 onTimer 方法从 sumOfTips 中删除相关的条目，这样做的效果是不可能容纳延迟的事件。 这相当于在使用 Flink 的时间窗口时将 allowedLateness 设置为零
    @Override
    public void onTimer(long timestamp,
        KeyedProcessFunction<Long, TaxiFare, Tuple3<Long, Long, Float>>.OnTimerContext ctx,
        Collector<Tuple3<Long, Long, Float>> out) throws Exception {
      Long driverId = ctx.getCurrentKey();
      // 查找刚结束的一小时的结果
      Float maxSumOfTips = this.sumOfTips.get(timestamp);
      Tuple3<Long, Long, Float> result = Tuple3.of(timestamp, driverId, maxSumOfTips);
      out.collect(result);
      sumOfTips.remove(timestamp);
    }
  }
}
