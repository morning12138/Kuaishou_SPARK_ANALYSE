#-*- coding: UTF-8 -*- 
import os
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import functions as F
import sys

# 保存到hdfs中
def outputSave(df):
    output_file = "hdfs://master:9000/user/ming1/kuaishou/gameBasicStatistics"
    df.coalesce(1).write.mode("append").options(header="true") \
        .csv(output_file, sep=",")


def describeColumn(sc, spark):
    # read csv file.
    df = spark.read.option("header", "true") \
        .option("inferSchema", "true") \
        .option("delimiter", ",") \
        .option("encoding", "utf-8") \
        .csv("hdfs://master:9000/user/ming1/kuaishou/游戏短视频数据(带评论).csv")

    # distinct by title
    # df = df.dropDuplicates(["title"])

    df = df.describe(["duration", "realLikeCount", "totcomment", "comrealLikedCount"])
    outputSave(df)


if __name__ == "__main__":
    # init
    reload(sys)  
    sys.setdefaultencoding('utf-8')
    conf = SparkConf().setAppName("describeColumn").setMaster("spark://master:7077")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)

    # exec functions
    describeColumn(sc, spark)
