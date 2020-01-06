package com.atguigu.spark.core.day01.singlevalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author atguigu
  * Date 2020/1/6 14:25
  */
object Sample {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Sample").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20, 11, 2, 3, 5)
        val rdd1: RDD[Int] = sc.parallelize(list1, 2)
        
        // 参数1: 是否放回  参数: 抽取的比例  如果是true [0, 无穷)  false [0,1]
        val rdd2: RDD[Int] = rdd1.sample(true, 1.1)
    
        rdd2.collect.foreach(println)
        sc.stop()
        
    }
}
