package com.hadp.mapred.chap8;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SortByHashPartitioner  extends Configured implements Tool
{
	@Override
	public int run(String[] args) throws Exception
	{
		Job job = new Job(getConf());

		job.setJarByClass(SortByHashPartitioner.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		
		/**
		 * 注意这里设置正确, 后面的查找操作才可以正常进行
		 */
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		

		return job.waitForCompletion(true) ? 0 : 1;

	}

	public static void main(String args[]) throws Exception
	{
		int exit = ToolRunner.run(new SortByHashPartitioner(), args);
		System.exit(exit);
	}

	
}
