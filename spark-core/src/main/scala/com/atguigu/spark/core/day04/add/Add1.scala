package com.atguigu.spark.core.day04.add

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Add1 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Add").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1, 4)
        
        val a: LongAccumulator = sc.longAccumulator("first")
        val rdd2: RDD[Int] = rdd1.map(x => {
            a.add(1)
            x
        })
        rdd2.collect
        println(a.value)
        sc.stop()
        
    }
}

/*
累加器:
    解决了共享变量写的问题!
 */