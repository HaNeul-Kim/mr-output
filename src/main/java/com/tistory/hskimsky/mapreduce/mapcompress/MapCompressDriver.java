package com.tistory.hskimsky.mapreduce.mapcompress;

import com.tistory.hskimsky.mapreduce.AbstractDriver;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author hskimsky
 * @version 1.0
 * @since 2020-12-28
 */
public class MapCompressDriver extends AbstractDriver {

  public static final String DRIVER_NAME = "mapcompress";

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      printUsage();
      System.exit(1);
    }
    System.exit(ToolRunner.run(new MapCompressDriver(), args));
  }

  private static void printUsage() {
    System.out.println("yarn jar mr-output-1.0-SNAPSHOT.jar " + DRIVER_NAME + " <INPUT_PATH> <OUTPUT_PATH>");
  }

  @Override
  protected String getJobName() {
    return DRIVER_NAME;
  }

  @Override
  protected Class<?> getDriverClass() {
    return MapCompressDriver.class;
  }

  @Override
  protected Class<? extends Mapper<LongWritable, Text, LongWritable, BytesWritable>> getMapperClass() {
    return MapCompressMapper.class;
  }

  @Override
  protected Class<? extends Reducer<LongWritable, BytesWritable, Text, NullWritable>> getReducerClass() {
    return MapCompressReducer.class;
  }
}
