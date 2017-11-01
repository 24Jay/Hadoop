package com.hadp.mapred.chap2;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class MaxTemperature
{
	public static void main(String[] ar) throws IOException
	{
		if (ar.length != 2)
		{
			System.out.println("Usage Error!");
			System.exit(-1);
		}
		JobConf conf = new JobConf(MaxTemperature.class);
		
		conf.addDefaultResource("mapred-site.xml");
		conf.setJobName("Max Temperature");

		FileInputFormat.addInputPath(conf, new Path(ar[0]));
		FileOutputFormat.setOutputPath(conf, new Path(ar[1]));
		

		conf.setMapperClass(TemperatureMapper.class);
		conf.setReducerClass(TemperatureReducer.class);
		conf.setOutputKeyClass(Text.class);

		conf.setOutputValueClass(IntWritable.class);
		JobClient.runJob(conf);
	}
}
