package org.apache.flink.processfunction;

/**
 * The data type stored in the state
 *
 * @author yinxing
 * @since 2023/11/8
 **/
public class CountWithTimestamp {

  public String key;

  public Long count;

  public Long lastModified;
}
