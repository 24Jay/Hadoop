package com.hadp.mapred.chap2;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hadp.mapred.NcdcRecord;

public class MultipleOutput extends Configured implements Tool
{

	static class MultipleMapper extends Mapper<LongWritable, Text, Text, Text>
	{

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException
		{
			NcdcRecord record = new NcdcRecord(value);
			String airTemperature = record.getAirTemperature()+"";
			context.write(new Text(record.getStation()), new Text(airTemperature));
		}

	}

	static class MultipleReducer extends Reducer<Text, Text, NullWritable, Text>
	{
		private MultipleOutputs<NullWritable, Text> outputs;

		@Override
		protected void cleanup(Reducer<Text, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException
		{
			outputs.close();
		}

		@Override
		protected void setup(Reducer<Text, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException
		{
			outputs = new MultipleOutputs<NullWritable, Text>(context);
		}

		@Override
		protected void reduce(Text arg0, Iterable<Text> arg1,
				Reducer<Text, Text, NullWritable, Text>.Context arg2)
				throws IOException, InterruptedException
		{
			for (Text va : arg1)
			{
				outputs.write(NullWritable.get(), va, arg0.toString());
			}
		}

	}

	@Override
	public int run(String[] args) throws Exception
	{
		Job job = new Job(getConf());

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		MultipleOutputs.addNamedOutput(job, args[2], TextOutputFormat.class, NullWritable.class, Text.class);
		job.setMapperClass(MultipleMapper.class);
		job.setMapOutputKeyClass(Text.class);

		job.setReducerClass(MultipleReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] ar)
	{
		int code = 1;
		try
		{
			code = ToolRunner.run(new MultipleOutput(), ar);
		}
		catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.exit(code);
	}
}