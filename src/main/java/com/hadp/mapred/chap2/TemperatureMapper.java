package com.hadp.mapred.chap2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.hadp.mapred.NcdcRecordParser;

/**
 * Hello world!
 *
 */
public class TemperatureMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>
{

	private NcdcRecordParser parser = new NcdcRecordParser();

	public void map(LongWritable arg0, Text arg1, OutputCollector<Text, IntWritable> arg2, Reporter arg3)
			throws IOException
	{
		String line = arg1.toString();
		if (line.length() < 93)
			return;

		parser.parse(line);

		if (parser.isValidTemperature())
		{
			String year = parser.getYear();
			int airTemperature = parser.getAirTemperature();
			arg2.collect(new Text(year), new IntWritable(airTemperature));
		}
	}
}
