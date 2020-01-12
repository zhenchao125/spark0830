package com.atguigu.sql.day02.datasource

import org.apache.spark.sql.SparkSession

/**
  * Author atguigu
  * Date 2020/1/12 11:17
  */
object Write {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("Write")
            .getOrCreate()
        import spark.implicits._
        val df = Seq(("lisi", 20), ("zs", 15)).toDF("name", "age")
        // 1. 通用的写
    
    
    
        // 2. 专用的写
    
    
        spark.close()
    }
}
