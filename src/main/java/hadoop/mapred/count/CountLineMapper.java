package hadoop.mapred.count;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountLineMapper extends Mapper<LongWritable, Text, LongWritable, Text>
{
	private static long counter = 0;

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context)
			throws IOException, InterruptedException
	{

		counter++;
		System.out.println("CountLineMapper ---->>> " + key + " counter=" + counter);
		String line = value.toString();
		if (line.length() == 0)
		{
			line = "HHHHHHHHHHHHH";
		}
		String head = line.substring(0, 5);
		context.write(key, new Text(head));
//		context.write(new LongWritable(counter), new Text(head));
	}

}
