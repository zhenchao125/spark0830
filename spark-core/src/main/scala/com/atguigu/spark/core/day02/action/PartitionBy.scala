package com.atguigu.spark.core.day02.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * Author atguigu
  * Date 2020/1/7 9:01
  */
object PartitionBy {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("PartitionBy").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1, 1)
        
        val rdd2: RDD[(Int, Int)] = rdd1.map((_, 1))
        // 隐式转换 RDD => PairRDDFunctions
        val rdd3: RDD[(Int, Int)] = rdd2.partitionBy(new HashPartitioner(2))
        
        val rdd4: RDD[Array[(Int, Int)]] = rdd3.glom()
        rdd4.collect.foreach(arr => println("a: " + arr.mkString(", ")))
        
        sc.stop()
        
    }
}
