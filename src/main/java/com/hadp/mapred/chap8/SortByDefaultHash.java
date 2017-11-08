package com.hadp.mapred.chap8;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/****
 * 部分排序: <br>
 * hadoop com.hadp.mapred.chap8.SortByDefaultHash -D mapred.reduce.tasks=5<br>
 * 
 * <br>
 * 全部排序: <br>
 * hadoop com.hadp.mapred.chap8.SortByDefaultHash -D mapred.reduce.tasks=1<br>
 * 
 * @author jay
 *
 */
public class SortByDefaultHash extends Configured implements Tool
{

	@Override
	public int run(String[] args) throws Exception
	{
		Job job = new Job(getConf());

		job.setJarByClass(SortByDefaultHash.class);

		/**
		 * 注意这里设置正确, 后面的查找操作才可以正常进行
		 */
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setOutputKeyClass(IntWritable.class);

		job.setPartitionerClass(HashPartitioner.class);

		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] ar) throws Exception
	{
		int exit = ToolRunner.run(new SortByDefaultHash(), ar);
		System.exit(exit);
	}

}
