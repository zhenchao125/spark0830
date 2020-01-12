package com.atguigu.sql.day02.datasource

import org.apache.spark.sql.{SaveMode, SparkSession}

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
//        df.write.format("json").save("c:/0830/user")
//        df.write.format("csv").mode("overwrite").save("c:/0830/csv/user")
        //mode=  保存模式
//        df.write.format("csv").mode(SaveMode.Overwrite).save("c:/0830/csv/user")
        
        // 2. 专用的写
        df.write.mode("overwrite").csv("c:/0830/csv/user")
        df.write.mode("overwrite").json("c:/0830/csv/user")
    
    
        spark.close()
    }
}
