package com.hadp.mapred.chap2;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTemperature
{
	public static void main(String[] ar) throws IOException, ClassNotFoundException, InterruptedException
	{
		if (ar.length != 2)
		{
			System.out.println("Usage Error!");
			System.exit(-1);
		}
		Job job = new Job();

		job.setJarByClass(MaxTemperature.class);
		job.setJobName("Max Temperature");
		FileInputFormat.addInputPath(job, new Path(ar[0]));
		FileOutputFormat.setOutputPath(job, new Path(ar[1]));

		job.setMapperClass(TemperatureMapper.class);
		job.setReducerClass(TemperatureReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
