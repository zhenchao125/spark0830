package com.atguigu.spark.core.day01.singlevalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author atguigu
  * Date 2020/1/6 14:35
  */
object Distinct {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Distinct").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10,70, 60, 10, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1, 2)
        
        val rdd2: RDD[Int] = rdd1.distinct()
        println(rdd2.collect.mkString(", "))
        
        
        sc.stop()
        
    }
}
