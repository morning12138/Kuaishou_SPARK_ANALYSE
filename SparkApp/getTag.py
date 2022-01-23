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


# 获得某一个标签相关的数据
def getTag(df, tag):
    where_s = "title like '%" + tag +"%'"
    print(where_s)
    df = df.select("*").where(where_s)

    return df


# 针对某一个tag的统计
def funOnTag(df, tag):
    # 标签与视频点赞数的关系
    tmp_df = df.dropDuplicates(["title"]).describe([ "title", "realLikeCount"])
    outputSave(tmp_df, tag+"_like")

    # 标签与评论数的关系
    tmp_df = df.dropDuplicates(["title"]).agg(F.sum("totcomment").alias("totalComment"))
    outputSave(tmp_df, tag+"_totalComment")

    # 不同视频的评论平均点赞数
    tmp_df = df.groupBy("title").agg(F.mean(df.comrealLikedCount).alias('mean_comrealLikedCount'))
    outputSave(tmp_df, tag+"_com_avg_like")


if __name__ == "__main__":
    # init
    reload(sys)
    sys.setdefaultencoding('utf-8')
    conf = SparkConf().setAppName("getTag").setMaster("spark://master:7077")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    df = spark.read.option("header", "true") \
        .option("inferSchema", "true") \
        .option("delimiter", ",") \
        .option("encoding", "utf-8") \
        .csv("hdfs://master:9000/user/ming1/kuaishou/result.csv")

    # exec functions

    tags = sc.textFile("hdfs://master:9000/user/ming1/kuaishou/tags.txt")
    tags_arr = tags.collect()

    for tag in tags_arr[0:4]:
        df1 = getTag(df, tag.encode('utf-8'))
        funOnTag(df1, tag.encode('utf-8'))
        # break


