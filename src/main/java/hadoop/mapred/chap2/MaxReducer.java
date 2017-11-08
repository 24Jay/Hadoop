package hadoop.mapred.chap2;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

class MaxReducer extends Reducer<Text, IntWritable, Text, IntWritable>
{
	private static long counter = 0;

	@Override
	protected void reduce(Text arg0, Iterable<IntWritable> arg1, Context arg2) throws IOException, InterruptedException
	{
		System.out.println("MaxReducer : counter = " + (++counter) + " key = " + arg0.toString());

		int maxValue = Integer.MIN_VALUE;
		int minValue = Integer.MAX_VALUE;

		System.out.println("*****************************************");
		for (IntWritable value : arg1)
		{
			int val = value.get();
			maxValue = Math.max(maxValue, val);
			minValue = Math.min(minValue, val);

			System.out.println("key = " + arg0.toString() + " max=" + maxValue + " min=" + minValue);
		}
		System.out.println("*****************************************\n\n");
		arg2.write(arg0, new IntWritable(maxValue));
	}

}

class MaxMinReducer extends Reducer<Text, IntWritable, Text, MapWritable>
{
	private static long counter = 0;

	@Override
	protected void reduce(Text arg0, Iterable<IntWritable> arg1, Context arg2) throws IOException, InterruptedException
	{

		System.out.println("MaxMinReducer : counter = " + (++counter) + " key = " + arg0.toString());
		int maxValue = Integer.MIN_VALUE;
		int minValue = Integer.MAX_VALUE;
		System.out.println("*****************************************");

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
			System.out.println("key = " + arg0.toString() + " max=" + maxValue + " min=" + minValue);
		}
		System.out.println("*****************************************\n\n");

		arg2.write(arg0, arrs);
	}

}