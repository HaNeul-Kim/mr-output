package com.tistory.hskimsky.mapreduce.writable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author hskimsky
 * @version 1.0
 * @since 2020-12-28
 */
public class WritableMapper extends Mapper<LongWritable, Text, LongWritable, YellowWritable> {

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    context.getCounter("CUSTOM", "MAP_READ_FILE").increment(1);
  }

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

    int passengerCount = tokens[3].length() > 0 ? Integer.parseInt(tokens[3]) : 0;
    float congestionSurcharge = tokens.length > 17 && tokens[17].length() > 0 ? Float.parseFloat(tokens[17]) : 0;

    YellowWritable yellowWritable = new YellowWritable();
    yellowWritable.setVendorID(tokens[0]);
    yellowWritable.setTpepPickupDatetime(tokens[1]);
    yellowWritable.setTpepDropoffDatetime(tokens[2]);
    yellowWritable.setPassengerCount(passengerCount);
    yellowWritable.setTripDistance(Float.parseFloat(tokens[4]));
    yellowWritable.setRateCodeID(tokens[5]);
    yellowWritable.setStoreAndFwdFlag(tokens[6]);
    yellowWritable.setPuLocationID(tokens[7]);
    yellowWritable.setDoLocationID(tokens[8]);
    yellowWritable.setPaymentType(tokens[9]);
    yellowWritable.setFareAmount(Float.parseFloat(tokens[10]));
    yellowWritable.setExtra(Float.parseFloat(tokens[11]));
    yellowWritable.setMtaTax(Float.parseFloat(tokens[12]));
    yellowWritable.setTipAmount(Float.parseFloat(tokens[13]));
    yellowWritable.setTollsAmount(Float.parseFloat(tokens[14]));
    yellowWritable.setImprovementSurcharge(Float.parseFloat(tokens[15]));
    yellowWritable.setTotalAmount(Float.parseFloat(tokens[16]));
    yellowWritable.setCongestionSurcharge(congestionSurcharge);

    context.getCounter("CUSTOM", "MAP_WRITE_RECORD").increment(1);
    context.write(new LongWritable(Arrays.hashCode(tokens)), yellowWritable);
  }
}
