package com.atguigu.spark.core.day01.doublevalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author atguigu
  * Date 2020/1/6 16:28
  */
object DoubleValue {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("DoubleValue").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 5, 7, 60, 10)
        val list2 = List(3, 5, 7, 6, 1, 2)
        val rdd1: RDD[Int] = sc.parallelize(list1, 2)
        val rdd2: RDD[Int] = sc.parallelize(list2, 2)
        
        //并集
        //        val rdd3: RDD[Int] = rdd1.union(rdd2)
        //        val rdd3: RDD[Int] = rdd1 ++ rdd2
        // 交集
        //        val rdd3: RDD[Int] = rdd1.intersection(rdd2)
        
        // 差集 rdd1 减去他们的交集
        //        val rdd3: RDD[Int] = rdd1.subtract(rdd2)
        
        // 拉链操作
        //        val rdd3: RDD[(Int, Int)] = rdd1.zip(rdd2)
        // 元素和自己的索引
        //        val rdd3: RDD[(Int, Long)] = rdd1.zipWithIndex()
        
        // 分区一样
        /*val rdd3 = rdd1.zipPartitions(rdd2)((it1: Iterator[Int], it2: Iterator[Int]) => {
            //            it1.zip(it2)  // 多余的扔掉
            it1.zipAll(it2, -1, -2) // 多余的地方补默认值
        })*/
        
        
        val rdd3 = rdd1.cartesian(rdd2)
        rdd3.collect.foreach(println)
        sc.stop()
        
    }
}

/*
zip:
    1. 分区数一样
    2. 每个分区内的元素个数也要一样 (总数一样)
 */