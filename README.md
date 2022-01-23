# SPARK_ANALYSE

[TOC]

## 1 项目介绍

### 1.1 目标

使用Spark对快手的视频数据进行分析。

### 1.2 使用的技术

* `pyspark`编写相关大数据分析算法。
  * 使用Spark SQL处理结构化数据

* 相关数据保存在`hdfs`中。
  * 爬虫数据保存为`csv`格式。
  * 均上传至hdfs中调用。

* 使用`本地集群`环境进行计算。
  * 分为master，slave1，slave2三个节点

## 2 文件结构

--DataSet

​	-- result.csv  // 爬虫爬下的数据

​	-- tags.txt // 获得的所有标签

--SparkApp

​	-- getTag.py // 根据不同的tag进行分析

​	-- videoTimeAndOther.py // 分析视频时长和喜爱数、评论数量的关系

​	-- describeColumn.py // 数据的基本统计信息

​	-- sortByVideoLike.py // 根据喜爱数排序

​	-- gameWordCount.py //游戏相关的词频统计

--Tools

​	-- deleteUnicode.py // 处理csv文件

​	-- getAllTag.py  // 处理csv文件，获得所有tags



## 3 已实现的算法

### 3.1 getTag

* 计算带有不同tag的视频的喜爱数量、评论数量。

* 计算带有不同tag的视频数量。

* 计算带有不同tag的评论的平均点赞数。

### 3.2 videoTimeAndOther

* 计算视频时长和喜爱度的相关度。
* 计算视频时长和评论数的相关度。

### 3.3 describeColumn

* 数据的基本统计信息，包括均值、方差、最大值、最小值等。

### 3.4 sortByVideoLike

* 根据喜爱数量对爬到的数据进行排序。

### 3.5 gameWordCount

* 对一系列的游戏相关词汇进行词频统计。

## 4 遇到的问题

1. [AttributeError: ‘PipelinedRDD‘ object has no attribute ‘toDF‘](https://stackoverflow.com/questions/32788387/pipelinedrdd-object-has-no-attribute-todf-in-pyspark)

   ```python
   from pyspark.sql import SparkSession
   sc = SparkContext(conf=conf)
   spark = SparkSession(sc)
   ```

2. **编码问题**

   PYTHON2默认不是utf-8，python3是，所以使用python2需要手动修改python的默认编码格式。否则会出现编码错误。

   ```python
   import sys
   reload(sys)  
   sys.setdefaultencoding('utf-8')
   df = spark.read.option("header", "true")\
       .option("inferSchema", "true") \
       .option("delimiter", ",") \
       .option("encoding", "utf-8") \
       .csv("hdfs://master:9000/user/ming1/kuaishou/test_kuaishou.csv")
   ```

   

