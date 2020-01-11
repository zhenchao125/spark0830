package com.atguigu.sql.day01

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Author atguigu
  * Date 2020/1/11 16:52
  */
object DF2DS {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
            .master("local[2]")
            .appName("Rdd2Df")
            .getOrCreate()
        import spark.implicits._
        val df: DataFrame = spark.read.json("c:/users.json")
        val ds: Dataset[User1] = df.as[User1]
        
        val df1: DataFrame = ds.toDF()
        df1.show
        
        
        spark.stop()
    }
}
case class User1(age: Long, name: String)
/*
df->ds
    df.as[样例类]
    
ds->df
    ds.toDF
    
3种数据类型, 6种转换
 


 */