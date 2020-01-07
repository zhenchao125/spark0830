package com.atguigu.spark.core.day02.action

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author atguigu
  * Date 2020/1/7 15:00
  */
object Cogroup {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Cogroup").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val rdd1 = sc.parallelize(Array((1, 10), (2, 20), (1, 100), (3, 30)), 1)
        val rdd2 = sc.parallelize(Array((1, "a"), (2, "b"), (1, "aa"), (3, "c")), 1)
        
        
        sc.stop()
        
    }
}
