package com.hadp.hdfs;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

public class FileCopyWithProgress
{
	public static void main(String[] ar) throws FileNotFoundException
	{
		String src = ar[0];
		String dst = ar[1];

		InputStream in = new BufferedInputStream(new FileInputStream(src));

		Configuration conf = new Configuration();
		try
		{
			FileSystem fs = FileSystem.get(URI.create(dst), conf);
			OutputStream out = fs.create(new Path(dst), new Progressable()
			{

				public void progress()
				{
					System.out.print("...");
				}
			});
			IOUtils.copyBytes(in, out, 4096, true);
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
