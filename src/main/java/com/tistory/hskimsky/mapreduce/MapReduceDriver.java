package com.tistory.hskimsky.mapreduce;

import com.tistory.hskimsky.mapreduce.flatbuf.FlatbufDriver;
import com.tistory.hskimsky.mapreduce.protobuf.ProtobufDriver;
import org.apache.hadoop.util.ProgramDriver;

/**
 * @author hskimsky
 * @version 1.0
 * @since 2020-12-21
 */
public class MapReduceDriver {

  public static void main(String[] args) throws Throwable {
    ProgramDriver programDriver = new ProgramDriver();
    programDriver.addClass(ProtobufDriver.DRIVER_NAME, ProtobufDriver.class, "use protocol buffer");
    programDriver.addClass(FlatbufDriver.DRIVER_NAME, FlatbufDriver.class, "use flat buffer");
    programDriver.driver(args);
  }
}
