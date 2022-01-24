#-*- coding: UTF-8 -*- 
import os
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import functions as F
import sys
reload(sys)  
sys.setdefaultencoding('utf-8')


# 获得某一个关键词的数据
def getWord(df, tag):
    where_s = "title like '%" + tag +"%'"
    print(where_s)
    df = df.dropDuplicates(["title"]).select("*").where(where_s)

    return df


# 针对某一个关键词的统计
def wordCount(df):
    # 词频统计
    return df.count()


if __name__ == "__main__":
    # init
    reload(sys)
    sys.setdefaultencoding('utf-8')
    conf = SparkConf().setAppName("hotWordCount").setMaster("spark://master:7077")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    df = spark.read.option("header", "true") \
        .option("inferSchema", "true") \
        .option("delimiter", ",") \
        .option("encoding", "utf-8") \
        .csv("hdfs://master:9000/user/ming1/kuaishou/热门短视频数据2w.csv")

    # exec functions

    words = sc.textFile("hdfs://master:9000/user/ming1/kuaishou/hot_tag.txt")
    words_arr = words.collect()
    dict = []
    for word in words_arr[5000:10000]:
        word = word[1:]
        df1 = getWord(df, word.encode('utf-8'))
        word_count = wordCount(df1)
        one = (word, str(word_count).encode('utf-8'))
        dict.append(one)

    word_count_df = spark.createDataFrame(dict, ["word", "wordCount"])
    word_count_df.coalesce(1).write.mode("append").options(header="true") \
        .csv("hdfs://master:9000/user/ming1/kuaishou/hotWordCount/", sep=",")
