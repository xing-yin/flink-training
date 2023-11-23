package org.apache.flink.processfunction;

import com.esotericsoftware.kryo.util.IntMap.Values;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction.Context;
import org.apache.flink.streaming.api.functions.ProcessFunction.OnTimerContext;
import org.apache.flink.streaming.api.functions.windowing.RichProcessWindowFunction;
import org.apache.flink.util.Collector;
import scala.Tuple2;

/**
 * <p>FIXME: 简要描述本文件的功能</p>
 *
 * @author yinxing
 * @since 2023/11/8
 **/
public class CountWithTimeoutFunction extends
    ProcessFunction<Tuple2<String, Long>, Tuple2<String, Long>> {

  private ValueState<CountWithTimestamp> state;

  @Override
  public void open(Configuration parameters) throws Exception {
    // register our state with the state backend
    state = getRuntimeContext().getState(
        new ValueStateDescriptor<>("myState", CountWithTimestamp.class));
  }

  @Override
  public void processElement(Tuple2<String, Long> value,
      Context ctx,
      Collector<Tuple2<String, Long>> out) throws Exception {
    // update our state and register a timer
    CountWithTimestamp current = state.value();
    if (current == null) {
      current = new CountWithTimestamp();
      current.key = value._1;
    }
    current.count++;
    current.lastModified = ctx.timestamp();
    state.update(current);
    ctx.timerService().registerEventTimeTimer(current.lastModified + 100);
  }

  @Override
  public void onTimer(long timestamp,
      ProcessFunction<Tuple2<String, Long>, Tuple2<String, Long>>.OnTimerContext ctx,
      Collector<Tuple2<String, Long>> out) throws Exception {
    // check the state for the key and emit a result if needed
    CountWithTimestamp result = state.value();
    if (timestamp == result.lastModified + 100) {
      out.collect(new Tuple2<>(result.key, result.count));
      state.clear();
    }
  }


}
