package com.atguigu.spark.core.day04.add

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Add {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Add").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1, 2)
        
        var a = 0
        val rdd2: RDD[Int] = rdd1.map(x => {
            a += 1
            x
        })
        rdd2.collect.foreach(println)
        sc.stop()
        
    }
}
