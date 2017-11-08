package hadoop.zkper;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.KeeperException;

public class DeleteGroup extends ConnectionWatcher
{
	public void delete(String groupName) throws KeeperException, InterruptedException
	{
		String path = "/"+groupName;
		
		List<String> children = zk.getChildren(path, false);
		
		for(String s:children)
			zk.delete(path+"/"+s, -1);
		zk.delete(path, -1);
	}
	
	public static void main(String[] ar) throws KeeperException, InterruptedException, IOException
	{
		DeleteGroup group = new DeleteGroup();
		group.connect(ar[0]);
		group.delete(ar[1]);
		group.close();
	}
}
