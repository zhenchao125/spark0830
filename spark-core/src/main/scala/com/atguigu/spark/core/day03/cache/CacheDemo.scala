package com.atguigu.spark.core.day03.cache

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author atguigu
  * Date 2020/1/8 11:02
  */
object CacheDemo {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("CacheDemo").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70)
        val rdd1: RDD[Int] = sc.parallelize(list1, 2)
        
        val rdd2 = rdd1.map(x => {
            println(x + "->")
            (x, 1)
        })
        val rdd3 = rdd2.filter(x => {
            println(x + "=>")
            true
        }).reduceByKey(_ + _)
        // 对rdd3做缓存 内存
//        rdd3.cache()
        rdd3.persist(StorageLevel.DISK_ONLY)
        rdd3.collect
        println("----------------")
        rdd3.collect
        rdd3.collect
        
        
        Thread.sleep(10000000)
        sc.stop()
        
    }
}
/*


 */