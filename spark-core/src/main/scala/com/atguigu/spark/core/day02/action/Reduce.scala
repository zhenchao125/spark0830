package com.atguigu.spark.core.day02.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author atguigu
  * Date 2020/1/7 16:29
  */
object Reduce {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Reduce").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1, 4)
//        val sum: Int = rdd1.reduce(_ + _)
        // 每个分区内用一次, 分区间聚合的时候再用一次   分区数 + 1
//        val sum = rdd1.aggregate(Int.MinValue)(_.max(_), _ + _) - Int.MinValue
//        val sum = rdd1.aggregate(1)(_ + _, _ + _)
        val sum = rdd1.fold(1)(_ + _)
        println(sum)
        sc.stop()
        
    }
}
/*
行动算子:
 reduce
 zeroValue参与的次数: 分区数 + 1
     fold
     aggregate
 count
 countByKey

 */