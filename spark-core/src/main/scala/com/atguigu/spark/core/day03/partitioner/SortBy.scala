package com.atguigu.spark.core.day03.partitioner

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author atguigu
  * Date 2020/1/8 14:53
  */
object SortBy {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("SortBy").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1, 2)
        rdd1.map(x => {
            println("x....")
            (x, 1)
        }).sortByKey()
        
        Thread.sleep(100000)
        sc.stop()
        
    }
}
