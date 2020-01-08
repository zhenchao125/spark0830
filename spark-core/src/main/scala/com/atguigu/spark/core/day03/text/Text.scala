package com.atguigu.spark.core.day03.text

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author atguigu
  * Date 2020/1/8 15:21
  */
object Text {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Text").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        /*val list1 = List(30, 50, 70, 60, 10, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1, 2)
        rdd1.saveAsTextFile("c:/0830")*/
        
        sc.textFile("c:/0508").collect
        
        sc.stop()
        
    }
}
