package com.atguigu.spark.core.day01.singlevalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author atguigu
  * Date 2020/1/6 14:12
  */
object GroupBy {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Glom").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 5, 70, 6, 1, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1, 2)
        
        val rdd2: RDD[(Int, Iterable[Int])] = rdd1.groupBy(x => x % 2)
        rdd2.collect.foreach(println)
        sc.stop()
        
    }
}
