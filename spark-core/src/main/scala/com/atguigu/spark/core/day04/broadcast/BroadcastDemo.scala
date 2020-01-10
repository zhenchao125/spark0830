package com.atguigu.spark.core.day04.broadcast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author atguigu
  * Date 2020/1/10 11:02
  */
object BroadcastDemo {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("BroadcastDemo").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        
        // 2 个executor, 4个分区
        val rdd1: RDD[Int] = sc.parallelize(List(30, 50, 70, 60, 10, 20, 30, 50, 10), 4)
        
        val set = Set(30, 50)
        val bc: Broadcast[Set[Int]] = sc.broadcast(set)
        val rdd2: RDD[Int] = rdd1.filter(x => bc.value.contains(x))
        rdd2.collect.foreach(println)
        
        sc.stop()
        
    }
}
/*
变量的共享问题
    累加器
        写

    广播变量
        大变量的读的问题
 */