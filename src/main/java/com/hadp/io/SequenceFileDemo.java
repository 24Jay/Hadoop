package com.hadp.io;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class SequenceFileDemo
{

	private static final String[] DATA = {

			"One, two, buckle my shoe", "Three, four, shut the door", "Five, six, pick up sticks",
			"Seven, eight, lay them straight", "Nine, ten, a big fat then" };

	private void write(String uri) throws IOException
	{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		Path path = new Path(uri);

		IntWritable key = new IntWritable();
		Text value = new Text();

		SequenceFile.Writer writer = null;

		// try (SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf,
		// path, IntWritable.class, Text.class))

		try
		{
			writer = SequenceFile.createWriter(fs, conf, path, IntWritable.class, Text.class);
			for (int i = 0; i < 100; i++)
			{
				key.set(100 - i);
				value.set(DATA[i % DATA.length]);
				System.out.println("--->>>" + writer.getLength() + " : key=" + key + ", value=" + value);
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
		Path path = new Path(uri);
		SequenceFile.Reader reader = null;

		try
		{
			reader = new SequenceFile.Reader(fs, path, conf);
			Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);

			long position = reader.getPosition();

			while (reader.next(key, value))
			{
				String syncSeen = reader.syncSeen() ? "*" : "";
				System.out.println(
						"+++++>>>>" + position + ", syncSeen=" + syncSeen + ", key=" + key + ", value=" + value);
				position = reader.getPosition();
			}

		}
		finally
		{
			IOUtils.closeStream(reader);
		}

	}

	public static void main(String[] ar) throws IOException
	{
		SequenceFileDemo demo = new SequenceFileDemo();
		System.out.println("\n********************************Write SequenceFile*************************************");
		demo.write(ar[0]);
		System.out.println("\n********************************Read SequenceFile*************************************");
		demo.read(ar[0]);
	}
}
