package com.hadp.mapred.chap8;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PartSort  extends Configured implements Tool
{
	@Override
	public int run(String[] args) throws Exception
	{
		Job job = new Job(getConf());

		job.setOutputKeyClass(IntWritable.class);
//		job.setOutputValueClass(Text.class);

//		job.setNumReduceTasks(0);
			
		
//		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputFormatClass(MapFileOutputFormat.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		
		
//		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		SequenceFileInputFormat.addInputPath(job, new Path(args[0]));
		SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));

		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);

		return job.waitForCompletion(true) ? 0 : 1;

	}

	public static void main(String args[]) throws Exception
	{
		int exit = ToolRunner.run(new ConvertText2Sequence(), args);
		System.exit(exit);
	}

	
}
