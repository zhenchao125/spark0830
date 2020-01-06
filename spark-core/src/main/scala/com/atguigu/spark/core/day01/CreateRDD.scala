package com.atguigu.spark.core.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author atguigu
  * Date 2020/1/6 9:56
  */
object CreateRDD {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("CreateRDD")
        val sc: SparkContext = new SparkContext(conf)
//        val arr1: Array[Int] = Array(30, 50, 70, 60, 10, 20)
//        val rdd1: RDD[Int] = sc.parallelize(arr1)
//        val rdd1: RDD[Int] = sc.makeRDD(arr1)
        val rdd1: RDD[Char] = sc.makeRDD("hello")
        
        rdd1.collect().foreach(println)
        sc.stop()
        
    }
}

/*

RDD编程:
    1. 先得到一个RDD
           1. 从文件中获取
                sc.textFile(...)
                
           2. 从现有的scala集合来获取一个RDD
           
           3. 通过其他的RDD转换得到(transformation)
    
    2. 对RDD做各种转换
        workcount:
                .flatMap.map.reduceByKey...
                
    3. 有一个action

 */