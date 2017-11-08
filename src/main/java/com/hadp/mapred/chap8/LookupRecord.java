package com.hadp.mapred.chap8;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hadp.mapred.NcdcRecord;

public class LookupRecord extends Configured implements Tool
{

	@Override
	public int run(String[] args) throws Exception
	{
		IntWritable key = new IntWritable(Integer.parseInt(args[1]));

		Reader[] readers = MapFileOutputFormat.getReaders(new Path(args[0]), getConf());

		Partitioner<IntWritable, Text> partitioner = new HashPartitioner<IntWritable, Text>();
		Text val = new Text();
		Writable entry = MapFileOutputFormat.getEntry(readers, partitioner, key, val);

		if (entry == null)
		{
			System.err.println("Key not found: " + key);
			return -1;
		}

		NcdcRecord record = new NcdcRecord(val);
		System.out.println("key = " + key.get() + ", station=" + record.getStation() + ", year=" + record.getYear());

		return 0;
	}

	
	public static void main(String []ar) throws Exception
	{
		int exit = ToolRunner.run(new LookupRecord(), ar);
		System.exit(exit);
	}
	
}
