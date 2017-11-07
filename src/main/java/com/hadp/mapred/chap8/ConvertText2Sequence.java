package com.hadp.mapred.chap8;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hadp.mapred.NcdcRecord;

public class ConvertText2Sequence extends Configured implements Tool
{
	static class SortMapper extends Mapper<LongWritable, Text, IntWritable, Text>
	{

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException
		{
			NcdcRecord record = new NcdcRecord(value);
			if (record.isValidTemperature())
			{
				context.write(new IntWritable(record.getAirTemperature()), value);
			}
		}

	}

	@Override
	public int run(String[] args) throws Exception
	{
		Job job = new Job(getConf());

		job.setMapperClass(SortMapper.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(0);

		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
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
