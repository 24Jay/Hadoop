package com.hadp;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * Hello world!
 *
 */
public class TemperatureMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>
{
	private static final int MISSING = 9999;

	public void map(LongWritable arg0, Text arg1, OutputCollector<Text, IntWritable> arg2, Reporter arg3)
			throws IOException
	{
		String line = arg1.toString();
		String year = line.substring(15, 19);

		if (line.length() < 87)
			return;

		int airTemperature;

		try
		{
			if (line.charAt(87) == '+')
			{
				airTemperature = Integer.parseInt(line.substring(88, 92));

			}
			else
			{
				airTemperature = Integer.parseInt(line.substring(87, 92));
			}
		}
		catch (NumberFormatException e)
		{
			System.out.println("*******************************************************");
			System.out.println(e);
			airTemperature = 0;
		}

		String quality = line.substring(92, 93);
		if (airTemperature != MISSING && quality.matches("[01459]"))
		{
			arg2.collect(new Text(year), new IntWritable(airTemperature));
		}
	}
}
