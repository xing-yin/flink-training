package org.apache.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * Operator 示例
 * <p>
 * https://nightlies.apache.org/flink/flink-docs-release-1.17/zh/docs/dev/datastream/operators/overview/
 *
 * @author yinxing
 * @since 2023/11/23
 **/
public class OperatorDemo {

  public static void main(String[] args) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStreamSource<String> dataStream = env.fromElements("DROP", "IGNORE", "name1,name2");

    // Map
    dataStream.map(new MapFunction<String, Integer>() {
      @Override
      public Integer map(String value) throws Exception {
        return Integer.valueOf(value);
      }
    });

    // FlatMap
    dataStream.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public void flatMap(String value, Collector<String> out) throws Exception {
        for (String word : value.split(",")) {
          out.collect(word);
        }
      }
    });

    // filter
    dataStream.filter(new FilterFunction<String>() {
      @Override
      public boolean filter(String value) throws Exception {
        return value.length() > 3;
      }
    });

    // keyBy: 在逻辑上将流划分为不相交的分区。具有相同 key 的记录都分配到同一个分区
    dataStream.keyBy((value -> value));

    // reduce: 在相同 key 的数据流上“滚动”执行 reduce。将当前元素与最后一次 reduce 得到的值组合然后输出新值。
    dataStream.keyBy((value -> value))
        .reduce(new ReduceFunction<String>() {
          @Override
          public String reduce(String value1, String value2) throws Exception {
            return value1 + value2;
          }
        });

    // window
    dataStream.keyBy((value -> value))
        .window(TumblingEventTimeWindows.of(Time.seconds(5)));

    // windowAll：Window 根据某些特征（例如，最近 5 秒内到达的数据）对所有流事件进行分组。
    dataStream.keyBy((value -> value))
        .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)));
  }
}
