package org.apache.flink.training.exercises.ridesandfares.domain;

import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.common.utils.GeoUtils;

/**
 * <p>FIXME: 简要描述本文件的功能</p>
 *
 * @author yinxing
 * @since 2023/11/21
 **/
public class EnrichRides extends TaxiRide {

  public int startCell;
  public int endCell;

  public EnrichRides() {
  }

  public EnrichRides(TaxiRide ride) {
    super();
    this.startCell = GeoUtils.mapToGridCell(ride.startLon, ride.startLat);
    this.endCell = GeoUtils.mapToGridCell(ride.endLon, ride.endLat);
  }

  @Override
  public String toString() {
    return super.toString() + "," +
        "startCell=" + startCell +
        ", endCell=" + endCell +
        '}';
  }
}
