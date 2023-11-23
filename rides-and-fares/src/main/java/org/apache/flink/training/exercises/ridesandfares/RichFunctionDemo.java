package org.apache.flink.training.exercises.ridesandfares;

import java.util.Objects;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * <p>FIXME: 简要描述本文件的功能</p>
 *
 * @author yinxing
 * @since 2023/11/21
 **/
public class RichFunctionDemo {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//    env.addSource(new EventSource())
//        .keyBy(e -> e.key)
//        .flatMap(new Deduplicator())
//        .print();

    env.execute();
  }

  private static class Event {

    public final String key;
    public final long timestamp;

    public Event(String key, long timestamp) {
      this.key = key;
      this.timestamp = timestamp;
    }
  }


  private static class Deduplicator extends RichFlatMapFunction<Event, Event> {

    // 部署在分布式集群时，将会有很多 Deduplicator 的实例，每一个实例将负责整个键空间的互斥子集中的一个。所以，当你看到一个单独的 ValueState
    // 这个代表的不仅仅是一个单独的布尔类型变量，而是一个分布式的共享键值存储。
    ValueState<Boolean> keyHasBeenSeen;

    @Override
    public void open(Configuration parameters) throws Exception {
      ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>("keyHasBeenSeen",
          Types.BOOLEAN);
      keyHasBeenSeen = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void flatMap(Event event, Collector<Event> out) throws Exception {
      Boolean value = keyHasBeenSeen.value();
      if (Objects.isNull(value)) {
        out.collect(event);
        keyHasBeenSeen.update(true);
      }
    }
  }
}
