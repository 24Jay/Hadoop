package com.hadp.hdfs;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/***
 * double cat 
 * @author jay
 *
 */
public class FileSystemCat
{

	public static void main(String[] ar) throws IOException
	{
		String uri = ar[0];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
//		InputStream in = null;
		FSDataInputStream in = null;
		try
		{
			in = fs.open(new Path(uri));
			IOUtils.copyBytes(in, System.out, 4096, false);
			/**
			 * print twice
			 */
			in.seek(0);
			IOUtils.copyBytes(in, System.out, 4096, false);
			
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		finally
		{
			IOUtils.closeStream(in);
		}

	}
}
