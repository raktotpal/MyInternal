package com.internal.sds.MR;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class compressionMapper extends Mapper<LongWritable, Text, NullWritable, IntWritable> {
  public void map(LongWritable key, Text input, Context output)
      throws IOException, InterruptedException {
    String Line = input.toString();
    Integer Line1 = Integer.parseInt(Line);
    output.write(NullWritable.get(), new IntWritable(Line1));
  }
}