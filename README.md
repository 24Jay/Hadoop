# Hadoop学习笔记
[参考知识总结](https://24jay.github.io/2017/10/24/IntroductionToHadoop/#more)

### 1. Hadoop

**1.1 MaxTemperature**
```shell
# 安装启动了HDFS之后, 引用本地文件必须使用file://...
hadoop jar target/hadp-0.0.1-SNAPSHOT.jar com.hadp.MaxTemperature file:///media/jay/Study/Hadoop/NCDC/ output
```
----


**1.2 URLCat**
针对本地文件
```shell
# FileSystemCat
$ hadoop jar target/hadp-0.0.1-SNAPSHOT.jar com.hadp.hdfs.FileSystemCat file:///etc/profile

$ hadoop jar target/hadp-0.0.1-SNAPSHOT.jar com.hadp.hdfs.URLCat file:///etc/profile
...

#for hadoop
export HADOOP_INSTALL=/opt/hadoop/hadoop-3.0.0-alpha4
export PATH=$PATH:$HADOOP_INSTALL/bin:$HADOOP_INSTALL/sbin

#for go
export GOROOT=/usr/local/go
export PATH=$PATH:$GOROOT/bin


#for selenium.webdriver.Chrome()
export CHROMEDRIVER=/opt/chromeDriver/
export PATH=$PATH:$CHROMEDRIVER

#for phantomjs
export PHANTOMJS=/opt/phantomjs/bin/
export PATH=$PATH:$PHANTOMJS

```
或者针对hdfs文件
```shell
$ hadoop jar target/hadp-0.0.1-SNAPSHOT.jar com.hadp.hdfs.URLCat hdfs://localhost/workers.sh
```


---

**1.3 ListStatus**
```shell
$ hadoop jar target/hadp-0.0.1-SNAPSHOT.jar com.hadp.hdfs.ListStatus hdfs://localhost/ hdfs://localhost/user/jay/
hdfs://localhost/profile
hdfs://localhost/user
hdfs://localhost/workers.sh
hdfs://localhost/user/jay/books
hdfs://localhost/user/jay/hadoop-daemon.sh
hdfs://localhost/user/jay/output
```

**1.4 写入数据: FileCopyWithProgress**
```shell
$ hadoop jar target/hadp-0.0.1-SNAPSHOT.jar com.hadp.hdfs.FileCopyWithProgress ~/hello.go hdfs://localhost/user/jay/hello.go
......
```

**1.5 通过distcp并行复制**
```shell
# distcp的典型应用场景是在两个HDFS集群之间传输数据
$ hadoop distcp hdfs://localhost/user/jay/hello.go hdfs://localhost/
$ hadoop distcp -update hdfs://localhost/user/jay/hello.go hdfs://localhost/
$ hadoop distcp -override hdfs://localhost/user/jay/hello.go hdfs://localhost/
```
--------
### 2. HBase

----

### 3. Zookeeper
**3.1 zookeeper命令行运行工具**
该命令行脚本在/opt/hadoop/zookeeper-3.4.10/bin目录下
```python
#首先可以查看具体支持哪些命令
$ zkCli.sh localhost
ZooKeeper -server host:port cmd args
	stat path [watch]
	set path data [version]
	ls path [watch]
	delquota [-n|-b] path
	ls2 path [watch]
	setAcl path acl
	setquota -n|-b val path
	history 
	redo cmdno
	printwatches on|off
	delete path [version]
	sync path
	listquota path
	rmr path
	get path [watch]
	create [-s] [-e] path data acl
	addauth scheme auth
	quit 
	getAcl path
	close 
	connect host:port


#例如, 列出某个znode之下的children
$  zkCli.sh -server localhost ls /
[zoo, zookeeper, hbase]

#获取znode的信息
zkCli.sh  -server localhost get /zoo/goat-0000000007
WatchedEvent state:SyncConnected type:None path:null
Hello Zookeeper
cZxid = 0xe8
ctime = Mon Oct 30 13:22:08 CST 2017
mZxid = 0xe8
mtime = Mon Oct 30 13:22:08 CST 2017
pZxid = 0xe8
cversion = 0
dataVersion = 0
aclVersion = 0
ephemeralOwner = 0x0
dataLength = 15
numChildren = 0
```

**3.2 JavaAPI操作zookeeper**
代码在com.hadp.zkper包中
```python
#在zoo节点下面创建duck
hadoop jar target/hadp-0.0.1-SNAPSHOT.jar com.hadp.zkper.CreateGroup localhost zoo/duck

#或者使用JoinGroup, 二者代码本质一样的
$ hadoop jar target/hadp-0.0.1-SNAPSHOT.jar com.hadp.zkper.JoinGroup localhost zoo goat

#列出zoo下面的znode
child------->>>>>>goat
child------->>>>>>duck

#删除group
hadoop jar target/hadp-0.0.1-SNAPSHOT.jar com.hadp.zkper.ListGroup localhost zoo/goat
hadoop jar target/hadp-0.0.1-SNAPSHOT.jar com.hadp.zkper.ListGroup localhost zoo
```
也可以使用以下方式运行
```python
~/Eclipse/workspace/hadoop/target/classes $ export CLASSPATH=/home/jay/Eclipse/workspace/hadoop/target/classes:$ZOOKEEPER_INSTALL/*:$ZOOKEEPER_INSTALL/lib/*:$ZOOKEEPER_INSTALL/conf
~/Eclipse/workspace/hadoop/target/classes$ java com.hadp.zkper.ListGroup localhost zoo
```

**3.3 使用zookeeper实现分布式Configer**
代码参考com.hadp.zkper.configExample
```python
#首先运行setter
hadoop jar target/hadp-0.0.1-SNAPSHOT.jar com.hadp.zkper.configExample.ConfigUpdater localhost

#然后运行watcher, 观察配置项目的变化
hadoop jar target/hadp-0.0.1-SNAPSHOT.jar com.hadp.zkper.configExample.ConfigWatcher localhost
```

