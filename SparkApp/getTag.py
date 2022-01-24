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
def outputSave(df):
    output_file = "hdfs://master:9000/user/ming1/kuaishou/gameTagAnalyse"
    df.coalesce(1).write.mode("append").options(header="true") \
        .csv(output_file, sep=",")


# 获得某一个标签相关的数据
def getTag(df, tag):
    where_s = "title like '%" + tag +"%'"
    # print(where_s)
    df = df.select("*").where(where_s)

    return df


# 针对某一个tag的统计
def funOnTag(df, tag):
    # 标签和喜爱数和评论数的关系
    df = df.dropDuplicates(["title"]).agg(F.sum("realLikeCount").alias("totalLike"), F.sum("totcomment").alias("totalComment"))
    # df.show()
    df1 = df.withColumn("tag", F.lit(str(tag)));
    return df1

    # 标签与评论数的关系
    # df2 = df.dropDuplicates(["title"]).agg(F.sum("totcomment").alias("totalComment")).show()

    # df_ret = df1.
    # outputSave(tmp_df, tag+"_totalComment")

    # 不同视频的评论平均点赞数
    # df2 = df.groupBy("title").agg(F.mean(df.comrealLikedCount).alias('mean_comrealLikedCount')).show()
    # outputSave(tmp_df, tag+"_com_avg_like")


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
        .csv("hdfs://master:9000/user/ming1/kuaishou/游戏短视频数据(带评论).csv")

    # exec functions

    tags = sc.textFile("hdfs://master:9000/user/ming1/kuaishou/game_tag.txt")
    tags_arr = tags.collect()
    df_save = spark.createDataFrame([(1, 1, 1)],["totalLike", "totalComment", "tag"])
    for tag in tags_arr:
        df1 = getTag(df, tag.encode('utf-8'))
        df_tmp = funOnTag(df1, tag.encode('utf-8'))
        df_save = df_save.union(df_tmp)
        # break
    # df_save.show()
    outputSave(df_save)



