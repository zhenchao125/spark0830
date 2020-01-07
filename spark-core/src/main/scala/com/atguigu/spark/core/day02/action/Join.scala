package com.atguigu.spark.core.day02.action

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author atguigu
  * Date 2020/1/7 14:38
  */
object Join {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Join").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        var rdd1 = sc.parallelize(Array((1, "a"), (1, "b"), (2, "c"), (4, "d")))
        val rdd2 = sc.parallelize(Array((1, "aa"), (3, "bb"), (2, "cc")))
        // 内连接
        //        val rdd3: RDD[(Int, (String, String))] = rdd1.join(rdd2)
        
        // (4, ("d", None))  (1, ("a", Some("aa")))
        //        val rdd3: RDD[(Int, (String, Option[String]))] = rdd1.leftOuterJoin(rdd2)
//        val rdd3 = rdd1.rightOuterJoin(rdd2)
        val rdd3 = rdd1.fullOuterJoin(rdd2)
        
        rdd3.collect.foreach(println)
        sc.stop()
        
    }
}
