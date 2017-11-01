package com.hadp.mapred.chap5;

import org.apache.hadoop.conf.Configuration;

public class PrintConfiguration
{

	public static void main(String []ar)
	{
		Configuration conf = new Configuration();
		conf.addResource("configuration-1.xml");
		/***
		 * 后面的配置文件会覆盖前面的, 但是final属性不会被覆盖
		 */
		conf.addResource("configuration-2.xml");
		System.out.println(">>>>>>>color="+conf.get("color"));
		System.out.println(">>>>>>>size="+conf.get("size"));
		System.out.println(">>>>>>>weight="+conf.get("weight"));
		System.out.println(">>>>>>>size-weight="+conf.get("size-weight"));
		/****
		 * 系统第一个读取默认的配置文件conf/core-site.xml,所以这里可以直接读取默认的配置数据<br>
		 * 所以默认配置文件中的数据可以被后面加载的配置文件覆盖
		 */
		System.out.println(">>>>>>>fs.default.name="+conf.get("fs.default.name"));
		System.out.println(">>>>>>>dfs.replication="+conf.get("dfs.replication"));
		Configuration.addDefaultResource("hdfs-site.xml");
		//或者是conf.addDefaultResource("hdfs-site.xml");
		System.out.println(">>>>>>>dfs.replication="+conf.get("dfs.replication"));
		System.out.println(">>>>>>>hostname="+conf.get("hostname"));
	}
}
