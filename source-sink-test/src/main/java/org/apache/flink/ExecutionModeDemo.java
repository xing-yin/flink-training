package org.apache.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * <p>FIXME: 简要描述本文件的功能</p>
 *
 * @author yinxing
 * @since 2023/11/23
 **/
public class ExecutionModeDemo {

  public static void main(String[] args) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // env.setRuntimeMode(RuntimeExecutionMode.BATCH); // 配置执行模式

    DataStream<String> dataStream = env
        .fromElements("DROP", "IGNORE")
        .keyBy(x -> x);

    dataStream
        .filter(value -> value.startsWith("D"))
        .map(new MyMapFunction1());
  }

  private static class MyMapFunction1 implements MapFunction<String, Integer> {

    @Override
    public Integer map(String value) throws Exception {
      return Integer.valueOf(value);
    }
  }

  private static class MyMapFunction2 extends RichMapFunction<String, Integer> {

    @Override
    public Integer map(String value) throws Exception {
      return Integer.valueOf(value);
    }
  }

}
