package com.atguigu.spark.core.day04.add

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object MyAddDemo {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Add").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20)
        val rdd1: RDD[Int] = sc.parallelize(list1, 4)
        
        val acc = new MapAcc
        // 使用之前要先把累计器注册到SparkContext上
        sc.register(acc, "myFirst")
        val rdd2: RDD[Int] = rdd1.map(x => {
            acc.add(x)
            x
        })
        rdd2.collect
        println(acc.value)
        
        Thread.sleep(1000000)
        sc.stop()
        
    }
}

/*
累加器:
    解决了共享变量写的问题!
    
    1. 定义累计器
    
    2. 创建累加器对象
    
    3. 注册
    
    4. 使用
 */