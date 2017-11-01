package com.hadp.mapred.chap2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Hello world!
 *
 */
public class TemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
	private NcdcRecordParser parser = new NcdcRecordParser();

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException
	{
		String line = value.toString();
		if (line.length() < 93)
			return;

		parser.parse(line);

		if (parser.isValidTemperature())
		{
			String year = parser.getYear();
			int airTemperature = parser.getAirTemperature();
			context.write(new Text(year), new IntWritable(airTemperature));
		}
	}

	
}
