package com.atguigu

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * Author atguigu
  * Date 2020/2/10 10:04
  */
object MapJoin {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("MapJoin").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20)
        val list2 = List(30, 50, 70, 30, 50, 20)
        val rdd1= sc.parallelize(list1, 2).map((_, 1))
        val rdd2= sc.parallelize(list2, 2).map((_, "a"))
       // rdd1.join(rdd2).collect.foreach(println)
        // 找一个小的RDD广播出去, 让后对另外一个RDDmap, 在map内完成join的逻辑
        val bdRDD2: Broadcast[Array[(Int, String)]] = sc.broadcast(rdd2.collect)
        val rdd3 = rdd1.flatMap{
            case (k, v) =>   // (30, 1)
                // (30, (1, "a")), (30, (1, "a"))
                val arrRDD2: Array[(Int, String)] = bdRDD2.value
            
                arrRDD2.filter(_._1 == k).map{
                    case (k2, v2) => (k2 , (v, v2))
                }
        }
        rdd3.collect.foreach(println)
        sc.stop()
        
    }
}
