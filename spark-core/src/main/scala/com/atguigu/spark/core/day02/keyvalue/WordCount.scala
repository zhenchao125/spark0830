package com.atguigu.spark.core.day02.keyvalue

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author atguigu
  * Date 2020/1/7 11:16
  */
object WordCount {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("FoldByKey").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val rdd = sc.parallelize(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)
        
        // 1.
        //        val rdd2: RDD[(String, Int)] = rdd.reduceByKey(_ + _)
        // 2.
        //        val rdd2: RDD[(String, Int)] = rdd.foldByKey(0)(_ + _)
        // 3.
        //        val rdd2: RDD[(String, Int)] = rdd.aggregateByKey(0)(_ + _, _ + _)
        // 4
        //        val rdd2 = rdd.groupByKey.map(t => (t._1, t._2.sum))
        //        val rdd2 = rdd.groupByKey.map(t => t match {
        //            case (key, it) => (key, it.sum)
        //        })
//        val rdd2 = rdd.groupByKey.map {
//            case (key, it) => (key, it.sum)
//        }
        // 5.
        val rdd2 = rdd.groupBy(_._1).map{
            case (key, it) => (key, it.map(_._2).sum)
        }
        rdd2.collect.foreach(println)
        
        sc.stop
        
    }
}
