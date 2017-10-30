package com.hadp.zkper;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;

public class JoinGroup extends ConnectionWatcher
{
	public void join(String groupName, String member) throws KeeperException, InterruptedException
	{
		String path = "/" + groupName + "/" + member;
		String createdPath = zk.create(path, null/* data */, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		System.out.println("Create Joined------->>>>" + createdPath);
	}

	
	public static void main(String[] ar) throws KeeperException, InterruptedException, IOException
	{
		JoinGroup group = new JoinGroup();
		group.connect(ar[0]);
		group.join(ar[1],ar[2]);
		Thread.sleep(100000);
		group.close();
	}
}
