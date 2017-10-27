# Hadoop学习笔记
[参考知识总结](https://24jay.github.io/2017/10/24/IntroductionToHadoop/#more)

### 1. MaxTemperature
```shell
# 安装启动了HDFS之后, 引用本地文件必须使用file://...
hadoop jar target/hadp-0.0.1-SNAPSHOT.jar com.hadp.MaxTemperature file:///media/jay/Study/Hadoop/NCDC/ output
```
----


### 2. URLCat
针对本地文件
```shell
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

### 3. ListStatus
```shell
$ hadoop jar target/hadp-0.0.1-SNAPSHOT.jar com.hadp.hdfs.ListStatus hdfs://localhost/ hdfs://localhost/user/jay/
hdfs://localhost/profile
hdfs://localhost/user
hdfs://localhost/workers.sh
hdfs://localhost/user/jay/books
hdfs://localhost/user/jay/hadoop-daemon.sh
hdfs://localhost/user/jay/output
```
