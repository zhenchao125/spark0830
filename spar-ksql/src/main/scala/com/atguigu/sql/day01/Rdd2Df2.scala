package com.atguigu.sql.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Author atguigu
  * Date 2020/1/11 15:42
  */
object Rdd2Df2 {
    def main(args: Array[String]): Unit = {
        // 1. 入口:  SparkSession
        val spark: SparkSession = SparkSession.builder()
            .master("local[2]")
            .appName("Rdd2Df")
            .getOrCreate()
        val list1: List[(String, Int)] = List(("lisi", 20), ("zs", 10))
        val rdd: RDD[(String, Int)] = spark.sparkContext.parallelize(list1)
        
        
        spark.stop()
    }
}

case class User(name: String, age: Int)

/*
import spark.implicits._
rdd->df
    1. rdd中存储是元组
        rdd.toDF("c1", "c2")
        
    2. rdd中存储是样例类
        rdd.toDF

 */