package com.atguigu.spark.core.day02.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author atguigu
  * Date 2020/1/7 14:16
  */
object SortByKey1 {
    // Ordered
    implicit val ord: Ordering[User] = new Ordering[User] {
        override def compare(x: User, y: User): Int = x.age - y.age
    }
    
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("FoldByKey").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val rdd: RDD[String] = sc.parallelize("hello world" :: "hello hello" :: "atguigu atguigu" :: Nil)
        val rdd1: RDD[(String, Int)] = rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
        // 按照单词的个数降序排列
        //        val rdd2: RDD[(String, Int)] = rdd1.sortBy(_._2, false)
        val rdd2 = rdd1
            .map { case (word, count) => (count, word) }
            .sortByKey(false)
            .map { case (count, word) => (word, count) }
        rdd2.collect.foreach(println)
        sc.stop
    }
}
