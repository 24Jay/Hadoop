package hadoop.mapred.chap5;

import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TestToolRunner extends Configured implements Tool
{
	static
	{
		Configuration.addDefaultResource("hdfs-site.xml");
		Configuration.addDefaultResource("mapred-site.xml");

	}

	@Override
	public int run(String[] arg0) throws Exception
	{
		Configuration conf = getConf();
		conf.addResource("configuration-1.xml");
		/***
		 * 后面的配置文件会覆盖前面的, 但是final属性不会被覆盖
		 */
		conf.addResource("configuration-2.xml");
		System.out.println(conf.get("mapred.job.tracker"));
		for (Entry<String, String> entry : conf)
		{
			System.out.println(entry.getKey() + " =====>>>> " + entry.getValue());
		}
		return 0;
	}

	/***
	 * 直接在eclipse上运行得到的，比命令行运行得到的配置项少很多
	 * @param ar
	 * @throws Exception
	 */
	public static void main(String[] ar) throws Exception
	{
		int exitCode = ToolRunner.run(new TestToolRunner(), ar);
		System.out.println(exitCode);
	}

}
