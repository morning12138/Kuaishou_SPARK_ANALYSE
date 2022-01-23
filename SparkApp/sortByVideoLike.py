#-*- coding: UTF-8 -*- 
import os
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import functions as F
import sys

# sort by realLikeCount
def sortByVideoLike(sc, spark):
    df = spark.read.option("header", "true")\
        .option("inferSchema", "true") \
        .option("delimiter", ",") \
        .option("encoding", "utf-8") \
        .csv("hdfs://master:9000/user/ming1/kuaishou/result.csv")
    
    # distinct by title
    df = df.dropDuplicates(["title"])
    df.filter( df.realLikeCount > 10000).show()


if __name__ == "__main__":
    # init
    reload(sys)  
    sys.setdefaultencoding('utf-8')
    conf = SparkConf().setAppName("sortByVideoLike").setMaster("spark://master:7077")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)

    # exec functions
    sortByVideoLike(sc, spark)
