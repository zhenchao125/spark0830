package com.atguigu.spark.core.day02.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author atguigu
  * Date 2020/1/7 10:16
  */
object AggregeteByKey {
    def main(args: Array[String]): Unit = {
        
        val conf: SparkConf = new SparkConf().setAppName("AggregeteByKey").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        
        val rdd = sc.parallelize(List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8)),2)
        
//        val rdd2: RDD[(String, Int)] = rdd.aggregateByKey(0)(_ + _, _ + _)
        // 每个key每个分区间的最大值的和
//         val rdd2 =rdd.aggregateByKey(Int.MinValue)((x, y) => x.max(y), (x, y) => x + y)
         val rdd2 =rdd.aggregateByKey(Int.MinValue)(_.max(_), _ + _)
        // 同时计算出来同一个key每个分区最大值和最小值的和   a -> (3, 2)   c -> (12, 10)   b ->(3, 3)
        
        rdd2.collect.foreach(println)
        sc.stop()
        
    }
}
