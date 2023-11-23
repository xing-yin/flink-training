package org.apache.flink.training.exercises.ridesandfares;

import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * <p>FIXME: 简要描述本文件的功能</p>
 *
 * @author yinxing
 * @since 2023/11/21
 **/
public class WatermarksDemo {


  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStream<Event> stream = env
        .fromElements(new Event("a", 122222))
        .keyBy(x -> x);

    WatermarkStrategy<Event> watermarkStrategy = WatermarkStrategy.<Event>forBoundedOutOfOrderness(
            Duration.ofSeconds(20))
        .withTimestampAssigner((event, timestamp) -> event.timestamp);

    DataStream<Event> withTimestampsAndWatermarks = stream.assignTimestampsAndWatermarks(
        watermarkStrategy);

  }

  private static class Event {

    public final String key;
    public final long timestamp;

    public Event(String key, long timestamp) {
      this.key = key;
      this.timestamp = timestamp;
    }
  }
}
