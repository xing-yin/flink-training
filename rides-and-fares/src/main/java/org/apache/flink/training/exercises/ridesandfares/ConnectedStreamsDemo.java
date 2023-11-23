package org.apache.flink.training.exercises.ridesandfares;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * 一个控制流是用来指定哪些词需要从 streamOfWords 里过滤掉的。
 * <p>
 * 参考：https://nightlies.apache.org/flink/flink-docs-release-1.17/zh/docs/learn-flink/etl/
 * <p>
 * RichCoFlatMapFunction 是一种可以被用于一对连接流的 FlatMapFunction，并且它可以调用 rich function 的接口。这意味着它可以是有状态的。
 * <p>
 * 布尔变量 blocked 被用于记录在数据流 control 中出现过的键（在这个例子中是单词），并且这些单词从 streamOfWords 过滤掉。这是 keyed
 * state，并且它是被两个流共享的，这也是为什么两个流必须有相同的键值空间。
 * <p>
 * 在 Flink 运行时中，flatMap1 和 flatMap2 在连接流有新元素到来时被调用 —— 在我们的例子中，control 流中的元素会进入
 * flatMap1，streamOfWords 中的元素会进入 flatMap2。这是由两个流连接的顺序决定的，本例中为 control.connect(streamOfWords)。
 *
 * @author yinxing
 * @since 2023/11/21
 **/
public class ConnectedStreamsDemo {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStream<String> control = env
        .fromElements("DROP", "IGNORE")
        .keyBy(x -> x);

    DataStream<String> streamOfWords = env
        .fromElements("Apache", "DROP", "Flink", "IGNORE")
        .keyBy(x -> x);

    control.connect(streamOfWords)
        .flatMap(new ControlFunction())
        .print();

    env.execute();
  }

  private static class ControlFunction extends RichCoFlatMapFunction<String, String, String> {

    private ValueState<Boolean> blocked;

    @Override
    public void open(Configuration parameters) throws Exception {
      blocked = getRuntimeContext().getState(new ValueStateDescriptor<>("blocked", Boolean.class));
    }

    @Override
    public void flatMap1(String value, Collector<String> out) throws Exception {
      blocked.update(Boolean.TRUE);
    }

    @Override
    public void flatMap2(String value, Collector<String> out) throws Exception {
      if (blocked.value() == null) {
        out.collect(value);
      }
    }
  }
}
