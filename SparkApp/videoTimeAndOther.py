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
    output_file = "hdfs://master:9000/user/ming1/kuaishou/hotVideoTimeAndOther"
    df.coalesce(1).write.mode("append").options(header="true") \
        .csv(output_file, sep=",")

# 分析视频时长和喜欢数的关系
def videoTimeAndLike(df):
    # 修改csv列的类型，否则无法计算协方差
    df = df.withColumn("duration",df.duration.astype("int"))
    df = df.withColumn("realLikeCount",df.realLikeCount.astype("int"))

    # print(df.corr("duration", "realLikeCount"))
    return df.corr("duration", "realLikeCount")


# 分析视频时长和评论数的关系
def videoTimeAndCommNum(df):
    # print(df.corr("totcomment", "duration"))
    return df.corr("totcomment", "duration")


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
        .csv("hdfs://master:9000/user/ming1/kuaishou/热门短视频数据2w.csv")
    
        
    # exec functions
    rel1 = videoTimeAndLike(df)
    # rel2 = videoTimeAndCommNum(df)
    dict = []
    one = [rel1]
    dict.append(one)
    df_save = spark.createDataFrame(dict, ["covariance_videotime_like"])
    outputSave(df_save)



