package com.atguigu.sql.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Author atguigu
  * Date 2020/1/11 15:42
  */
object Df2Rdd {
    def main(args: Array[String]): Unit = {
        // 1. 入口:  SparkSession
        val spark: SparkSession = SparkSession.builder()
            .master("local[2]")
            .appName("Rdd2Df")
            .getOrCreate()
        
        val df: DataFrame = spark.read.json("c:/users.json")
        val rdd: RDD[Row] = df.rdd
        
        val rdd1 = rdd.map(row => {
            User(row.getString(1), row.getLong(0).toInt)
        })
    
        rdd1.collect.foreach(println)
        spark.stop()
    }
}


/*
import spark.implicits._
rdd->df
    1. rdd中存储是元组
        rdd.toDF("c1", "c2")
        
    2. rdd中存储是样例类
        rdd.toDF
        
    3. 使用SparkSession提供的原始api
        park.createDataFrame(rdd, schema)
        
df->rdd
    val rdd :RDD[Row] =
  df.rdd

 */