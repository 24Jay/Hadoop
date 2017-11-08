package hadoop.hdfs;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class ListStatus
{

	public static void main(String[] ar) throws IOException
	{
		String uri = ar[0];

		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), configuration);

		Path[] paths = new Path[ar.length];
		for (int i = 0; i < ar.length; i++)
		{
			paths[i] = new Path(ar[i]);
		}

		FileStatus[] status = fs.listStatus(paths);
		Path[] ps = FileUtil.stat2Paths(status);

		for (Path p : ps)
			System.out.println(p);
	}
}
