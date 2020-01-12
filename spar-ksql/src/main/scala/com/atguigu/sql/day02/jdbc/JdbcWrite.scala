package com.atguigu.sql.day02.jdbc

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
  * Author atguigu
  * Date 2020/1/12 11:36
  */
object JdbcWrite {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("Write")
            .getOrCreate()
        import spark.implicits._
        val df = Seq(("lisi", 20), ("zs", 15)).toDF("name", "age")
        
        // 1. 通用的写
        /*df.write.format("jdbc")
            .option("url", "jdbc:mysql://hadoop102:3306/rdd")
            .option("user", "root")
            .option("password", "aaaaaa")
            .option("dbtable", "user2")
            .mode("append")
            .save()*/
        
        // 2. 专用的写
        val props = new Properties()
        props.put("user", "root")
        props.put("password", "aaaaaa")
        df.write
            .mode("append")
            .jdbc("jdbc:mysql://hadoop102:3306/rdd", "user2", props)
        
        spark.stop()
        
        
    }
}
