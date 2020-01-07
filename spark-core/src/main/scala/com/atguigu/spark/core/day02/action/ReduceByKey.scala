package com.atguigu.spark.core.day02.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author atguigu
  * Date 2020/1/7 9:23
  */
object ReduceByKey {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("ReduceByKey").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val rdd1 = sc.parallelize(List(("female", 1), ("male", 5), ("female", 1), ("female", 5), ("male", 2), ("male", 2))) // female-> 6  male-> 7
        // map段聚合 combine
        val rdd2: RDD[(String, Int)] = rdd1.reduceByKey(_ + _)
        rdd2.collect.foreach(println)
        sc.stop()
        
    }
}
