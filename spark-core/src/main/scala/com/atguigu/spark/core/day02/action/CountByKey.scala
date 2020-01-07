package com.atguigu.spark.core.day02.action

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author atguigu
  * Date 2020/1/7 16:17
  */
object CountByKey {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("CountByKey").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20)
        val rdd1= sc.parallelize(list1, 2).map((_, 1))
        // reduceByKey是转换算子  countByKey行动算子
        val map: collection.Map[Int, Long] = rdd1.countByKey()
        println(map)
        sc.stop()
        
    }
}
