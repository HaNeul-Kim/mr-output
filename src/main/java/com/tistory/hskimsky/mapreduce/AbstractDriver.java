package com.tistory.hskimsky.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author hskimsky
 * @version 1.0
 * @since 2020-12-21
 */
public abstract class AbstractDriver extends Configured implements Tool {

  private static final Logger logger = LoggerFactory.getLogger(AbstractDriver.class);

  protected abstract String getJobName();

  protected abstract Class<?> getDriverClass();

  protected abstract Class<? extends Mapper<LongWritable, Text, LongWritable, BytesWritable>> getMapperClass();

  protected abstract Class<? extends Reducer<LongWritable, BytesWritable, Text, NullWritable>> getReducerClass();

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

    Job job = Job.getInstance(conf, this.getJobName());
    job.setJarByClass(this.getDriverClass());
    // input, output
    TextInputFormat.addInputPath(job, inputPath);
    TextOutputFormat.setOutputPath(job, outputPath);
    job.setInputFormatClass(TextInputFormat.class);
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(BytesWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    // mapper
    job.setMapperClass(this.getMapperClass());
    // combiner
    // partitioner
    // reducer
    job.setNumReduceTasks(tasks);
    job.setReducerClass(this.getReducerClass());

    return job.waitForCompletion(true) ? 0 : 1;
  }

  private Configuration newConf(int mapMemory, int redMemory, double slowstart) {
    Configuration conf = new Configuration();

    conf.set("fs.defaultFS", "hdfs://nn");
    conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
    conf.setDouble("mapreduce.job.reduce.slowstart.completedmaps", slowstart);

    conf.setInt(MRJobConfig.MAP_MEMORY_MB, mapMemory);
    conf.set(MRJobConfig.MAP_JAVA_OPTS, "-Xmx" + (int) (mapMemory * 0.8) + "m");
    conf.setInt(MRJobConfig.REDUCE_MEMORY_MB, redMemory);
    conf.set(MRJobConfig.REDUCE_JAVA_OPTS, "-Xmx" + (int) (redMemory * 0.8) + "m");
    conf.set(MRJobConfig.MAP_OUTPUT_COMPRESS, "true");
    conf.set(MRJobConfig.MAP_OUTPUT_COMPRESS_CODEC, SnappyCodec.class.getName());

    return conf;
  }
}
