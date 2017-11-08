package hadoop.mapred.chap8;

import java.io.IOException;

import org.apache.commons.lang.ObjectUtils.Null;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

import hadoop.mapred.NcdcRecord;

public class MaxTemperatureBySecondarySort extends Configured implements Tool
{

	static class MaxTemperatureMapper extends Mapper<LongWritable, Text, IntPair, NullWritable>
	{

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, IntPair, NullWritable>.Context context)
				throws IOException, InterruptedException
		{
			NcdcRecord record = new NcdcRecord(value);
			int first = Integer.parseInt(record.getYear());
			if (record.isValidTemperature())
			{
				context.write(new IntPair(first, record.getAirTemperature()), NullWritable.get());
			}

		}

	}

	static class MaxTemperatureReducer extends Reducer<IntPair, NullWritable, IntPair, NullWritable>
	{

		@Override
		protected void reduce(IntPair arg0, Iterable<NullWritable> arg1,
				Reducer<IntPair, NullWritable, IntPair, NullWritable>.Context arg2)
				throws IOException, InterruptedException
		{
			arg2.write(arg0, NullWritable.get());
		}

	}

	static class FirstPartitioner extends Partitioner<IntPair, NullWritable>
	{

		@Override
		public int getPartition(IntPair key, NullWritable value, int numPartitions)
		{
			return Math.abs(key.getFirst()*127)%numPartitions;
		}
		
	}
	
	
	@Override
	public int run(String[] args) throws Exception
	{

		return 0;
	}

}

class IntPair
{
	private int first;

	private int second;

	public IntPair(int year, int temp)
	{
		this.first = year;
		this.second = temp;
	}

	public int getFirst()
	{
		return first;
	}

	public void setFirst(int first)
	{
		this.first = first;
	}

	public int getSecond()
	{
		return second;
	}

	public void setSecond(int second)
	{
		this.second = second;
	}
	
	
}
