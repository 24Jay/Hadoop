package com.hadp.mapred.chap8;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TotalSortByTotalOrderPartitioner extends Configured implements Tool
{

	@Override
	public int run(String[] args) throws Exception
	{
		Job job = new Job(getConf());

		job.setJarByClass(TotalSortByTotalOrderPartitioner.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setOutputKeyClass(IntWritable.class);
		
		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);

		job.setPartitionerClass(TotalOrderPartitioner.class);
		
		InputSampler.Sampler<IntWritable, Text> sampler = new InputSampler.RandomSampler<IntWritable, Text>(0.1, 10000,10);
		InputSampler.writePartitionFile(job, sampler);
		

		Configuration configuration = job.getConfiguration();
		String partitionFile = TotalOrderPartitioner.getPartitionFile(configuration);
		URI  uri = new URI(partitionFile+"#"+TotalOrderPartitioner.DEFAULT_PATH);	

		DistributedCache.addCacheFile(uri, configuration);
		DistributedCache.createSymlink(configuration);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] ar) throws Exception
	{
		int exit = ToolRunner.run(new TotalSortByTotalOrderPartitioner(), ar);
		System.exit(exit);
	}

}

