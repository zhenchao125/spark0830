package com.atguigu.sql.day01

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

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
        val rdd = spark.sparkContext.parallelize(list1).map {
            case (name, age) =>
                Row(name, age)
        }
        
        //        val schema = StructType(List(StructField("name", StringType), StructField("age", IntegerType) ))
        val schema = StructType(StructField("name", StringType) :: StructField("age", IntegerType) :: Nil)
        val df: DataFrame = spark.createDataFrame(rdd, schema)
        
        df.show(100)
        
        spark.stop()
    }
}


/*
import spark.implicits._
rdd->df
    1. rdd中存储是元组
        rdd.toDF("c1", "c2")
        
    2. rdd中存储是样例类(√)
        rdd.toDF
    
    3. 使用SparkSession提供的原始api
        park.createDataFrame(rdd, schema)

 */