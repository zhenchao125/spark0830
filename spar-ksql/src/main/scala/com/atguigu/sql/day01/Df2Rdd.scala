package com.atguigu.sql.day01

import org.apache.spark.sql.{DataFrame, SparkSession}

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
        import spark.implicits._
        // 2. 创建df(把rdd转换df)
        val list1 = List(User("lisi", 20), User("zs", 10))
        val rdd= spark.sparkContext.parallelize(list1)
        val df: DataFrame = rdd.toDF
        // 3. 查询df
        df.show
        spark.createDataFrame()
        // 4. 关闭session
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

 */