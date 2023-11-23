package org.apache.flink.sideoutput;

import java.util.Collections;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction.Context;
import org.apache.flink.streaming.api.scala.DataStream;
import org.apache.flink.streaming.api.scala.OutputTag;
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * SideOutput 示例演示
 *
 * @author yinxing
 * @since 2023/11/8
 **/
public class SideOutputDemo {

  public static void main(String[] args) {
//    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//    DataStream<Object> dataStream = env.fromCollection();
//    OutputTag<String> sideOutputTag = new OutputTag<String>("side") {
//    };
//    SingleOutputStreamOperator<Integer> passThroughtStream = dataStream.process(
//        new ProcessFunction<Integer, Integer>() {
//          @Override
//          public void processElement(Integer value, ProcessFunction<Integer, Integer>.Context ctx,
//              Collector<Integer> out) throws Exception {
//            out.collect(value);
//            ctx.output(sideOutputTag":sideout-" + String.valueOf(value));
//          }
//        });
//
//    passThroughtStream.getSideOutput(sideOutputTag).addSink(sideOutputResultSink);
  }

}
