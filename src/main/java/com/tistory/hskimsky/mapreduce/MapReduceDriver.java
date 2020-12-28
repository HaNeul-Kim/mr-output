package com.tistory.hskimsky.mapreduce;

import com.tistory.hskimsky.mapreduce.flatbuf.FlatbufDriver;
import com.tistory.hskimsky.mapreduce.mapcompress.MapCompressDriver;
import com.tistory.hskimsky.mapreduce.plain.PlainDriver;
import com.tistory.hskimsky.mapreduce.protobuf.ProtobufDriver;
import com.tistory.hskimsky.mapreduce.writable.WritableDriver;
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
    programDriver.addClass(PlainDriver.DRIVER_NAME, PlainDriver.class, "use plain");
    programDriver.addClass(MapCompressDriver.DRIVER_NAME, MapCompressDriver.class, "use map compress");
    programDriver.addClass(WritableDriver.DRIVER_NAME, WritableDriver.class, "use writable");
    programDriver.driver(args);
  }
}
