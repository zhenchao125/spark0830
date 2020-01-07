package com.atguigu.spark.core.day02.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author atguigu
  * Date 2020/1/7 9:23
  */
object GroupByKey {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("GroupByKey").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val rdd1: RDD[(String, Int)] = sc.parallelize(List(("female", 1), ("male", 5), ("female", 1), ("female", 5), ("male", 2), ("male", 2))) // female-> 6  male-> 7
        //        val rdd2: RDD[(String, Iterable[Int])] = rdd1.groupByKey()
        //        val rdd2 = rdd1.groupBy(_._1).map(t => (t._1, t._2.map(_._2)))\\
        val rdd2 = rdd1.groupBy(_._1).map {
            case (key, it) =>
                (key, it.map(_._2))
                
        }
        // RDD[(a,(b,(c,(d, e))))]  match
        rdd2.collect.foreach(println)
        sc.stop()
        
    }
}
