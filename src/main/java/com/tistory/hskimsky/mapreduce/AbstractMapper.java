package com.tistory.hskimsky.mapreduce;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author hskimsky
 * @version 1.0
 * @since 2020-12-21
 */
public abstract class AbstractMapper extends Mapper<LongWritable, Text, LongWritable, BytesWritable> {

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    context.getCounter("CUSTOM", "MAP_READ_FILE").increment(1);
  }

  protected abstract byte[] getBytes(String[] tokens);

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    context.getCounter("CUSTOM", "MAP_READ_LINE").increment(1);
    long lineNumber = key.get();
    if (lineNumber == 0) {
      context.getCounter("CUSTOM", "MAP_READ_HEADER").increment(1);
      return;
    }
    context.getCounter("CUSTOM", "MAP_READ_RECORD").increment(1);

    String[] tokens = value.toString().split(",", -1);
    if (tokens.length < 17) {
      context.getCounter("CUSTOM", "MAP_SKIP_RECORD").increment(1);
      return;
    }
    byte[] bytes = this.getBytes(tokens);

    context.getCounter("CUSTOM", "MAP_WRITE_RECORD").increment(1);
    context.write(new LongWritable(Arrays.hashCode(bytes)), new BytesWritable(bytes));
  }
}
