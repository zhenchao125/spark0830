package com.atguigu.sql.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Author atguigu
  * Date 2020/1/11 16:38
  */
object RDD2DS {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("CreateDS")
            .getOrCreate()
        import spark.implicits._
        val rdd = spark.sparkContext.parallelize(Seq(Person("lisi", 20), Person("zs", 21)))
        val ds: Dataset[Person] = rdd.toDS()
        
        val rdd1: RDD[Person] = ds.rdd
        rdd1.collect.foreach(println)
        spark.close()
        
        
    }
}
/*
df能用的方法, ds都可以用

创建ds:
    先有样例类
    把样例类存入集合中, 调用集合的toDS()(利用了隐式转换)
    
rdd->ds
    样rdd存储样例类, 然后直接转
    
ds->rdd
    ds.rdd
    

 */