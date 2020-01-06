package com.atguigu.spark.core.day01.singlevalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author atguigu
  * Date 2020/1/6 10:19
  */
object MapPartitions {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Map").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20, 100)
        val rdd1: RDD[Int] = sc.parallelize(list1, 3)
        
        //        val resultRDD = rdd1.mapPartitions(it => it.map(x => x * x))
        
       /*
        val resultRDD = rdd1.map(x => {
            // 建立到mysql的连接
            
            // 从mysql读数据
            
        })
        
        rdd1.mapPartitions(it => {
            // 建立到mysql的连接
            // 从mysql读数据
            
            it
        })*/
    
        val resultRDD = rdd1.mapPartitionsWithIndex((index, it) => {
//            it.map((index, _))
            it.map(x => (index, x))
        })
        
        println(resultRDD.collect().mkString(", "))
        
        sc.stop()
        
    }
}
