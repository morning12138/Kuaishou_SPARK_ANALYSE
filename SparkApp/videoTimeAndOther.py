#-*- coding: UTF-8 -*- 
import os
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import functions as F
import sys
reload(sys)  
sys.setdefaultencoding('utf-8')

# 保存到hdfs中
def outputSave(df, tag):
    output_file = "hdfs://master:9000/user/ming1/kuaishou/tagAnalyse/" + tag[1:]
    df.coalesce(1).write.mode("append").options(header="true") \
        .csv(output_file, sep=",")

# 分析视频时长和喜欢数的关系
def videoTimeAndLike(df):
    # 修改csv列的类型，否则无法计算协方差
    df = df.withColumn("duration",df.duration.astype("int"))
    df = df.withColumn("realLikeCount",df.realLikeCount.astype("int"))

    print(df.corr("duration", "realLikeCount"))


# 分析视频时长和评论数的关系
def videoTimeAndCommNum(df):
    print(df.corr("totcomment", "duration"))


if __name__ == "__main__":
    # init
    reload(sys)
    sys.setdefaultencoding('utf-8')
    conf = SparkConf().setAppName("videoTimeAndLike").setMaster("spark://master:7077")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    df = spark.read.option("header", "true") \
        .option("inferSchema", "true") \
        .option("delimiter", ",") \
        .option("encoding", "utf-8") \
        .csv("hdfs://master:9000/user/ming1/kuaishou/result.csv")
    
        
    # exec functions
    videoTimeAndLike(df)
    videoTimeAndCommNum(df)



