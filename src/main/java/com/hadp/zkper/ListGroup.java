package com.hadp.zkper;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.KeeperException;

public class ListGroup extends ConnectionWatcher
{
	public void list(String groupName)
	{
		String path = "/"+groupName;
		
		try
		{
			List<String> children = zk.getChildren(path, false);
		
			if(children.isEmpty())
			{	System.out.println("No member in group "+groupName);
				System.exit(1);
			}
			
			for(String s:children)
				System.out.println("child------->>>>>>"+s);
		}
		catch (KeeperException e)
		{

			e.printStackTrace();
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}
		
	}
	
	
	public static void main(String[] ar) throws KeeperException, InterruptedException, IOException
	{
		ListGroup group = new ListGroup();
		group.connect(ar[0]);
		group.list(ar[1]);
		group.close();
	}
}
