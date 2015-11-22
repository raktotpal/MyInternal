package com.internal.sds.MR;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class testCompression extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("Error: Insufficient Parameter...!!!");
			System.exit(-1);
		}
		FileSystem fs = FileSystem.get(new Configuration());
		if (fs.exists(new Path(args[1]))) {
			System.out.println("The  Output Path Already Exists");
			boolean isDeleted = fs.delete(new Path(args[1]), true);
			if (isDeleted == true) {
				System.out.println("The  Output Path Is Now Deleted");
			}
		}
		// the ToolRunner.run static method returns the status for the
		// map-reduce task.
		int result = ToolRunner.run(new Configuration(), new testCompression(),
				args);
		System.exit(result);
	}

	public int run(String[] arg0) throws Exception {
		@SuppressWarnings("deprecation")
		Job job = new Job();
		job.setJarByClass(testCompression.class);
		job.setJobName("SDS-TEST");
		job.setMapperClass(compressionMapper.class);
		job.setReducerClass(compressionReducer.class);
		
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
		
		
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
}