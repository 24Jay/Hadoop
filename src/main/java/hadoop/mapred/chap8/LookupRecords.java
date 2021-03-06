package hadoop.mapred.chap8;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import hadoop.mapred.NcdcRecord;

public class LookupRecords extends Configured implements Tool
{

	@Override
	public int run(String[] args) throws Exception
	{
		IntWritable key = new IntWritable(Integer.parseInt(args[1]));

		// 为MapRecude作业创建的每一个输出文件分别打开一个MapFile.Reader实例
		Reader[] readers = MapFileOutputFormat.getReaders(new Path(args[0]), getConf());
		System.out.println("LookupRecords--->>>readers.length=" + readers.length);

		Partitioner<IntWritable, Text> partitioner = new HashPartitioner<IntWritable, Text>();

		Text val = new Text();

		int partitionNo = partitioner.getPartition(key, val, readers.length);
		System.out.println("LookupRecords--->>>partitioner.getPartition(" + key.get() + ")=" + partitionNo);

		Reader reader = readers[partitionNo];
		Writable entry = reader.get(key, val);

		if (entry == null)
		{
			System.err.println("Key not found: " + key);
			return -1;
		}

		IntWritable nextKey = new IntWritable();

		do
		{
			NcdcRecord record = new NcdcRecord(val);
			System.out.println("key=" + key.get() + ", station=" + record.getStation() + ", year=" + record.getYear());
		}
		while (reader.next(nextKey, val) && key.equals(nextKey));

		return 0;
	}

	public static void main(String[] ar) throws Exception
	{
		int exit = ToolRunner.run(new LookupRecords(), ar);
		System.exit(exit);
	}

}
