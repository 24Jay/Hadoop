package com.hadp.mapred.chap2;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable>
{

	@Override
	protected void reduce(Text arg0, Iterable<IntWritable> arg1,
			Reducer<Text, IntWritable, Text, IntWritable>.Context arg2) throws IOException, InterruptedException
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
