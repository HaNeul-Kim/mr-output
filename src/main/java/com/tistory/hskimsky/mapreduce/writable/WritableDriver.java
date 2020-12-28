package com.tistory.hskimsky.mapreduce.writable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author hskimsky
 * @version 1.0
 * @since 2020-12-28
 */
public class WritableDriver extends Configured implements Tool {

  private static final Logger logger = LoggerFactory.getLogger(WritableDriver.class);

  public static final String DRIVER_NAME = "writable";

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      printUsage();
      System.exit(1);
    }
    System.exit(ToolRunner.run(new WritableDriver(), args));
  }

  private static void printUsage() {
    System.out.println("yarn jar mr-output-1.0-SNAPSHOT.jar " + DRIVER_NAME + " <INPUT_PATH> <OUTPUT_PATH>");
  }

  @Override
  public int run(String[] args) throws Exception {
    Path inputPath = new Path(args[0]);
    Path outputPath = new Path(args[1]);
    int tasks = Integer.parseInt(args[2]);
    int mapMemory = args.length > 3 ? Integer.parseInt(args[3]) : 2048;
    int redMemory = args.length > 4 ? Integer.parseInt(args[4]) : 2048;
    double slowstart = args.length > 5 ? Double.parseDouble(args[5]) : 1.0;

    Configuration conf = this.newConf(mapMemory, redMemory, slowstart);

    FileSystem fs = outputPath.getFileSystem(conf);
    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true);
      logger.warn("deleted {}", outputPath);
    }

    Job job = this.getJob(conf, inputPath, outputPath, tasks);
    return job.waitForCompletion(true) ? 0 : 1;
  }

  protected Configuration newConf(int mapMemory, int redMemory, double slowstart) {
    Configuration conf = new Configuration();

    conf.set("fs.defaultFS", "hdfs://nn");
    conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
    conf.setDouble("mapreduce.job.reduce.slowstart.completedmaps", slowstart);

    conf.setInt(MRJobConfig.MAP_MEMORY_MB, mapMemory);
    conf.set(MRJobConfig.MAP_JAVA_OPTS, "-Xmx" + (int) (mapMemory * 0.8) + "m");
    conf.setInt(MRJobConfig.REDUCE_MEMORY_MB, redMemory);
    conf.set(MRJobConfig.REDUCE_JAVA_OPTS, "-Xmx" + (int) (redMemory * 0.8) + "m");
    conf.setBoolean(MRJobConfig.MAP_OUTPUT_COMPRESS, true);
    conf.set(MRJobConfig.MAP_OUTPUT_COMPRESS_CODEC, SnappyCodec.class.getName());

    return conf;
  }

  protected Job getJob(Configuration conf, Path inputPath, Path outputPath, int tasks) throws IOException {
    Job job = Job.getInstance(conf, DRIVER_NAME);
    job.setJarByClass(WritableDriver.class);
    // input, output
    TextInputFormat.addInputPath(job, inputPath);
    TextOutputFormat.setOutputPath(job, outputPath);
    job.setInputFormatClass(TextInputFormat.class);
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(YellowWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    // mapper
    job.setMapperClass(WritableMapper.class);
    // combiner
    // partitioner
    // reducer
    job.setNumReduceTasks(tasks);
    job.setReducerClass(WritableReducer.class);
    return job;
  }
}
