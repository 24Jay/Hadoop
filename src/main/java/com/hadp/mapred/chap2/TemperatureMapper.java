package com.hadp.mapred.chap2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.hadp.mapred.NcdcRecord;

public class TemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
	private static long counter = 0;

	/***
	 * 用户自己定义的计数器, 可以在最后的总结统计输出中看到
	 * @author jay
	 *
	 */
	enum Temperature
	{
		MISSING, MALFORMED
	}

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException
	{
		System.out.println("TemperatureMapper : counter = " + (++counter) + " key = " + key.get());

		NcdcRecord record = new NcdcRecord(value);

		if (record.isMalformed())
		{
			context.getCounter(Temperature.MALFORMED).increment(1);
		}
		else if (record.isMissing())
		{
			context.getCounter(Temperature.MISSING).increment(1);
		}
		else
		{
			String year = record.getYear();
			int airTemperature = record.getAirTemperature();
			context.write(new Text(year), new IntWritable(airTemperature));
			context.getCounter("TemperatureQuality", record.getQuality()).increment(1);
		}
		System.out.println("TemperatureMapperKey = " + key);
	}

}
