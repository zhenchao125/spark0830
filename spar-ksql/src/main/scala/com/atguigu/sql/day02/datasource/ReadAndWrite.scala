package com.atguigu.sql.day02.datasource

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author atguigu
  * Date 2020/1/12 8:36
  */
object ReadAndWrite {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("ReadAndWrite")
            .getOrCreate()
        import spark.implicits._
        
        // 通用的读方法  默认格式(没有format)是parquet
//        val df: DataFrame = spark.read.format("json").load("c:/users.json")
//        val df: DataFrame = spark.read.format("csv").load("c:/users.csv").toDF("c1", "c2", "c3")
        // 专用的读方法
//        val df = spark.read.json("c:/users.json")  // 等价于: spark.read.format("json").load("c:/users.json")
        val df = spark.read.csv("c:/users.csv")  // 等价于: spark.read.format("csv").load("c:/users.csv")
        df.show
        spark.close()
        
        
    }
}
/*
读:
    spark.read.format().load("路径")


 */