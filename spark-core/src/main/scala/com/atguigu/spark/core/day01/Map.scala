package com.atguigu.spark.core.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author atguigu
  * Date 2020/1/6 10:19
  */
object Map {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Map").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1)
        
        // map 用的最多, 调整数据结构
        val resultRDD = rdd1.map(math.pow(_, 2))
        println(resultRDD.collect().mkString(", "))
        
        sc.stop()
        
    }
}
