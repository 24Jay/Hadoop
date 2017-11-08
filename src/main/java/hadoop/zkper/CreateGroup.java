package hadoop.zkper;

import java.io.IOException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;

public class CreateGroup extends ConnectionWatcher
{
	

	public void create(String groupName) throws KeeperException, InterruptedException
	{
		String path = "/" + groupName;
		String createdPath = zk.create(path, "Hello Zookeeper".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
		System.out.println("Created------->>>>" + createdPath);
	}

	
	
	public void close() throws InterruptedException
	{
		zk.close();
	}

	public static void main(String[] ar) throws KeeperException, InterruptedException, IOException
	{
		CreateGroup group = new CreateGroup();
		group.connect(ar[0]);
		group.create(ar[1]);
		
		Thread.sleep(10000);
		group.close();
	}

}
