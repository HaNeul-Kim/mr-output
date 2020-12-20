package com.tistory.hskimsky.mapreduce;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author hskimsky
 * @version 1.0
 * @since 2020-12-21
 */
public abstract class AbstractReducer<T> extends Reducer<LongWritable, BytesWritable, Text, NullWritable> {

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    context.getCounter("CUSTOM", "RED_READ_FILE").increment(1);
  }

  protected abstract T getObject(byte[] value);

  @Override
  protected void reduce(LongWritable key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
    context.getCounter("CUSTOM", "RED_READ_GROUP").increment(1);
    long count = 0;
    for (BytesWritable value : values) {
      context.getCounter("CUSTOM", "RED_READ_RECORD").increment(1);
      T object = this.getObject(value.copyBytes());
      count++;
    }
    context.getCounter("CUSTOM", "RED_WRITE_GROUP").increment(1);
    context.getCounter("CUSTOM", "RED_WRITE_RECORD").increment(count);
    // context.write(new Text(String.valueOf(outputValues.size())), NullWritable.get());
  }
}
