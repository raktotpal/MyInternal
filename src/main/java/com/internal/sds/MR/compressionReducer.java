package com.internal.sds.MR;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class compressionReducer extends
    Reducer<NullWritable, IntWritable, NullWritable, IntWritable> {

  public void reduce(NullWritable key, Iterable<IntWritable> values, Context output)
      throws IOException, InterruptedException {
    int MaxVal = Integer.MIN_VALUE;
    for (IntWritable value : values) {
      MaxVal = Math.max(MaxVal, value.get());
    }
    output.write(NullWritable.get(), new IntWritable(MaxVal));
  }
}