package com.hadp.mapred.chap2;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CountLine
{	
	public static void main(String []ar) throws IOException, ClassNotFoundException, InterruptedException
	{
		if(ar.length != 2)
		{
			System.out.println("Usage Error!");
		}
		
		Job job = new Job();

		job.setJarByClass(CountLine.class);
		job.setJobName("Count Total Lines");
		
		FileInputFormat.addInputPath(job, new Path(ar[0]));
		FileOutputFormat.setOutputPath(job, new Path(ar[1]));
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(CountLineMapper.class);
//		job.setReducerClass(CountLineReducer.class);
		System.out.println(job.waitForCompletion(true)?0:1);
	}
}
