package com.hadp.io;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;

public class MapFileDemo
{

	private static final String[] DATA = {

			"One, two, buckle my shoe", "Three, four, shut the door", "Five, six, pick up sticks",
			"Seven, eight, lay them straight", "Nine, ten, a big fat then" };

	private void write(String uri) throws IOException
	{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);

		IntWritable key = new IntWritable();
		Text value = new Text();

		MapFile.Writer writer = null;

		// try (SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf,
		// path, IntWritable.class, Text.class))

		try
		{
			writer = new MapFile.Writer(conf, fs, uri, IntWritable.class, Text.class);
			for (int i = 1; i <= 1024; i++)
			{
				key.set(i);
				value.set(DATA[i % DATA.length]);
				System.out.println("--->>>" + writer.getIndexInterval() + " : key=" + key + ", value=" + value);
				writer.append(key, value);
			}
		}
		finally
		{
			IOUtils.closeStream(writer);
		}

	}

	private void read(String uri) throws IOException
	{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		MapFile.Reader reader = null;

		try
		{
			reader = new MapFile.Reader(fs, uri, conf);
			// Writable key = (Writable)
			// ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			// Writable value = (Writable)
			// ReflectionUtils.newInstance(reader.getValueClass(), conf);

			IntWritable key = new IntWritable();
			Text value = new Text();

			while (reader.next(key, value))
			{
				System.out.println("+++++>>>> key=" + key + ", value=" + value);
			}

		}
		finally
		{
			IOUtils.closeStream(reader);
		}

	}

	public static void main(String[] ar) throws IOException
	{
		MapFileDemo demo = new MapFileDemo();
		System.out.println("\n********************************Write MapFile*************************************");
		demo.write(ar[0]);
		System.out.println("\n********************************Read MapFile*************************************");
		demo.read(ar[0]);
	}

}
