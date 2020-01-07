package com.atguigu.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author atguigu
  * Date 2020/1/4 16:40
  */
object Hello {
    def main(args: Array[String]): Unit = {
        // 1. 创建 SparkContext  (如果在linux通过spark-submit提交, 一定要去掉setMaster. )
        val conf: SparkConf = new SparkConf().setAppName("Hello")
        val sc: SparkContext = new SparkContext(conf)
        
        // 2. 创建 RDD
        val rdd1: RDD[String] = sc.textFile("hdfs://hadoop102:9000/input")
        // 3. 对RDD左各种转换操作
        val wordAndCount: RDD[(String, Int)] = rdd1.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
        
        // 4. 执行一个行动算子
        val result: Array[(String, Int)] = wordAndCount.collect() // 把在executor执行的结果, 拉倒驱动端.
        
        result.foreach(println)
        
        // 5. 关闭SparkContext
        sc.stop()
        
        
        val list1 = List(30, 50, 70, 60, 10, 20)
        
        //ab
        // 原始
    }
}