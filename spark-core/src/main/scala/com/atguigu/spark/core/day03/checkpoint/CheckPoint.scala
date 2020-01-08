package com.atguigu.spark.core.day03.checkpoint

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
  * Author atguigu
  * Date 2020/1/8 11:32
  */
object CheckPoint {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("CacheDemo").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        sc.setCheckpointDir("./ck1")
        val list1 = List(30)
        val rdd1: RDD[Int] = sc.parallelize(list1, 2)
    
        val rdd2 = rdd1.map(x => {
//            println(x + "->")
            (x, System.currentTimeMillis())
        })
        val rdd3 = rdd2.filter(x => {
//            println(x + "=>")
            true
        })
        rdd3.cache()
        rdd3.checkpoint()
    
        println(rdd3.collect.mkString(","))
        println("----------------")
        println(rdd3.collect.mkString(","))
//
        println("----------------")
        println(rdd3.collect.mkString(","))
        println(rdd3.collect.mkString(","))
        println(rdd3.collect.mkString(","))
        println(rdd3.collect.mkString(","))

    
    
        Thread.sleep(10000000)
        sc.stop()
        
    }
    
}
