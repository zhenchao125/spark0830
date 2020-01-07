package com.atguigu.spark.core.day02.action

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author atguigu
  * Date 2020/1/7 16:17
  */
object Foreach {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("CountByKey").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20)
        val rdd1 = sc.parallelize(list1, 2).map((_, 1))
        //       rdd1.collect().foreach(println)
//        rdd1.foreach(println)
        // rdd的数据写入到外部存储比如: mysql
        // 1. rdd1.collect() 先把数据拉倒驱动端, 再从驱动端向mysql
        // 2. 分区内的数据, 直接向mysql写
       /* rdd1.foreach(x => {
            // 建立到mysql的连接
            // 写
        })*/
        rdd1.foreachPartition(it => {
            // 建立到mysql的连接
            // 写
        })
        sc.stop()
        
    }
}
