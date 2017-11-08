package hadoop.mapred.count;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountLineReducer extends Reducer<LongWritable, Text, LongWritable, Text>
{
	private static long counter = 0;

	@Override
	protected void reduce(LongWritable arg0, Iterable<Text> arg1,
			Reducer<LongWritable, Text, LongWritable, Text>.Context arg2) throws IOException, InterruptedException
	{
		System.out.println("CountLineReducer ---->>> " + (++counter));

		Text val = new Text("NULL");
		int count = 0;

		for (Text value : arg1)
		{
			val = value;
			System.out.println("Reducer Iterator: " + val + " = " + (++count));
		}
		arg2.write(arg0, val);
	}

}
