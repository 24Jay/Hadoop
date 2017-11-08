package hadoop.zkper.configExample;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;

public class ConfigUpdater
{

	public static final String PATH = "/config";
	
	private ActiveKeyValuesStore store;
	
	private Random random = new Random();
	
	public ConfigUpdater(String hosts) throws IOException, InterruptedException
	{
		this.store = new ActiveKeyValuesStore();
		store.connect(hosts);
	}
	
	public void run() throws KeeperException, InterruptedException
	{
		while(true)
		{
			String value = random.nextInt(100)+"";
			store.write(PATH, value);
			System.out.println("Set "+PATH+" to "+value);
			TimeUnit.SECONDS.sleep(random.nextInt(10));
		}
	}
	
	
	public static void main(String []ar) throws IOException, InterruptedException, KeeperException
	{
		ConfigUpdater updater = new ConfigUpdater(ar[0]);
		updater.run();
	}
}
