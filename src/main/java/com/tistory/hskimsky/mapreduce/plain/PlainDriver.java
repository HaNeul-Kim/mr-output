package com.tistory.hskimsky.mapreduce.plain;

import com.tistory.hskimsky.mapreduce.AbstractDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author hskimsky
 * @version 1.0
 * @since 2020-12-28
 */
public class PlainDriver extends AbstractDriver {

  public static final String DRIVER_NAME = "plain";

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      printUsage();
      System.exit(1);
    }
    System.exit(ToolRunner.run(new PlainDriver(), args));
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
    return PlainDriver.class;
  }

  @Override
  protected Class<? extends Mapper<LongWritable, Text, LongWritable, BytesWritable>> getMapperClass() {
    return PlainMapper.class;
  }

  @Override
  protected Class<? extends Reducer<LongWritable, BytesWritable, Text, NullWritable>> getReducerClass() {
    return PlainReducer.class;
  }

  @Override
  protected Configuration newConf(int mapMemory, int redMemory, double slowstart) {
    Configuration conf = super.newConf(mapMemory, redMemory, slowstart);
    conf.setBoolean(MRJobConfig.MAP_OUTPUT_COMPRESS, false);
    return conf;
  }
}
