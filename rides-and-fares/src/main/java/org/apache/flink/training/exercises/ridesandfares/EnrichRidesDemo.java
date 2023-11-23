package org.apache.flink.training.exercises.ridesandfares;

import org.apache.commons.math3.geometry.euclidean.oned.Interval;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel.ID;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.training.exercises.common.datatypes.RideAndFare;
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.common.sources.TaxiFareGenerator;
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator;
import org.apache.flink.training.exercises.common.utils.GeoUtils;
import org.apache.flink.training.exercises.ridesandfares.domain.EnrichRides;
import org.apache.flink.util.Collector;
import scala.Tuple2;

/**
 * <p>FIXME: 简要描述本文件的功能</p>
 *
 * @author yinxing
 * @since 2023/11/21
 **/
public class EnrichRidesDemo {

  private final SourceFunction<TaxiRide> rideSource;
  private final SourceFunction<TaxiFare> fareSource;
  private final SinkFunction<RideAndFare> sink;

  public EnrichRidesDemo(SourceFunction<TaxiRide> rideSource, SourceFunction<TaxiFare> fareSource,
      SinkFunction<RideAndFare> sink) {
    this.rideSource = rideSource;
    this.fareSource = fareSource;
    this.sink = sink;
  }

  public static void main(String[] args) {

    EnrichRidesDemo job =
        new EnrichRidesDemo(
            new TaxiRideGenerator(),
            new TaxiFareGenerator(),
            new PrintSinkFunction<>());

    job.execute();
  }

  private void execute() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // 演示 map()
    DataStream<TaxiRide> rides = env.addSource(rideSource);
    DataStream<EnrichRides> enrichedNYCRides = rides
        //.filter(new RideCleansingSolution.NYCFilter)
        .map(new Enrichment());

    // 演示 flatMap()
    DataStream<EnrichRides> enrichedNYCRides2 = rides.flatMap(new NYCEnrichment());
    enrichedNYCRides2.print();

    // 演示 flatMap()
    KeyedStream<EnrichRides, Integer> keyedStream = rides.flatMap(
            new NYCEnrichment())
        .keyBy(enrichedNYCRides3 -> enrichedNYCRides3.startCell);
//        .keyBy(enrichedNYCRides3 -> GeoUtils.mapToGridCell(enrichedNYCRides3.startLon,enrichedNYCRides3.startLat)); // 更好的选择

    // 演示 Keyed Stream 的聚合: 为每个行程结束事件创建了一个新的包含 startCell 和时长（分钟）的元组流：
//    DataStream<Tuple2<Integer, Minutes>> minutesByStartCell = enrichedNYCRides.flatMap(
//        new FlatMapFunction<EnrichRides, Tuple2<Integer, Minutes>>() {
//
//          @Override
//          public void flatMap(EnrichRides ride, Collector<Tuple2<Integer, Minutes>> out)
//              throws Exception {
//            if (!ride.isStart) {
//              Interval rideInterval = new Interval(ride.startTime, ride.endTime);
//              Minutes duration = rideInterval.toDuration().toStandardMinutes();
//              out.collect(new Tuple2<>(ride.startCell, duration));
//            }
//          }
//        });

  }

  // MapFunction 只适用于一对一的转换：对每个进入算子的流元素，map() 将仅输出一个转换后的元素。对于除此以外的场景，你将要使用 flatmap()。
  private class Enrichment implements MapFunction<TaxiRide, EnrichRides> {

    @Override
    public EnrichRides map(TaxiRide taxiRide) throws Exception {
      return new EnrichRides(taxiRide);
    }
  }


  private class NYCEnrichment implements FlatMapFunction<TaxiRide, EnrichRides> {

    @Override
    public void flatMap(TaxiRide taxiRide, Collector<EnrichRides> out) throws Exception {
      FilterFunction<TaxiRide> valid = new NYCFilter();
      if (valid.filter(taxiRide)) {
        out.collect(new EnrichRides(taxiRide));
      }
    }

    public class NYCFilter implements FilterFunction<TaxiRide> {

      @Override
      public boolean filter(TaxiRide taxiRide) throws Exception {
        return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat) && GeoUtils.isInNYC(
            taxiRide.endLon, taxiRide.endLat);
      }
    }
  }
}
