package com.hadp.zkper.configExample;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;

public class ConfigWatcher implements Watcher
{
	private ActiveKeyValuesStore store;

	public ConfigWatcher(String hosts) throws IOException, InterruptedException
	{
		this.store = new ActiveKeyValuesStore();
		store.connect(hosts);

	}

	public void displayConfig() throws KeeperException, InterruptedException
	{
		String value = store.read(ConfigUpdater.PATH, this);
		System.out.println("Read------>>>>>" + ConfigUpdater.PATH + " = " + value);
	}

	@Override
	public void process(WatchedEvent event)
	{
		if (event.getType() == EventType.NodeDataChanged)
		{
			try
			{
				displayConfig();
			}
			catch (KeeperException e)
			{
				System.out.println("KeeperException.....Exiting." + e);
			}
			catch (InterruptedException e)
			{
				System.err.println("Inerrupted ... Exiting.");
				Thread.currentThread().interrupt();
			}
		}

	}
	
	
	public static void main(String []ar) throws IOException, InterruptedException, KeeperException
	{
		ConfigWatcher watcher = new ConfigWatcher(ar[0]);
		watcher.displayConfig();
		Thread.sleep(100000);
	}
	
	
}
