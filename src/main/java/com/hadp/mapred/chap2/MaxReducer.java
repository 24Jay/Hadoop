package com.hadp.mapred.chap2;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class MaxReducer extends Reducer<Text, IntWritable, Text, IntWritable>
{

	@Override
	protected void reduce(Text arg0, Iterable<IntWritable> arg1, Context arg2) throws IOException, InterruptedException
	{
		int maxValue = Integer.MIN_VALUE;
		int minValue = Integer.MAX_VALUE;

		for (IntWritable value : arg1)
		{
			int val = value.get();
			maxValue = Math.max(maxValue, val);
			minValue = Math.min(minValue, val);
		}
		arg2.write(arg0, new IntWritable(maxValue));
	}

}

class MaxMinReducer extends Reducer<Text, IntWritable, Text, MapWritable>
{

	@Override
	protected void reduce(Text arg0, Iterable<IntWritable> arg1, Context arg2) throws IOException, InterruptedException
	{
		int maxValue = Integer.MIN_VALUE;
		int minValue = Integer.MAX_VALUE;

		MapWritable arrs = new MapWritable();
		Text max = new Text("Max");
		Text min = new Text("Min");
		for (IntWritable value : arg1)
		{
			int val = value.get();
			maxValue = Math.max(maxValue, val);
			minValue = Math.min(minValue, val);

			arrs.put(max, new FloatWritable(maxValue / 10));
			arrs.put(min, new FloatWritable(minValue / 10));
		}
		arg2.write(arg0, arrs);
	}

}