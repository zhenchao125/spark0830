package com.atguigu.spark.core.day01.singlevalue

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author atguigu
  * Date 2020/1/6 15:13
  */
object Coalease {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Coalease").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1, 3)
    
        println(rdd1.getNumPartitions)
//        val rdd2: RDD[Int] = rdd1.coalesce(2, true)
        val rdd2: RDD[Int] = rdd1.repartition(4)
        println(rdd2.getNumPartitions)
        
        sc.stop()
        
    }
}
