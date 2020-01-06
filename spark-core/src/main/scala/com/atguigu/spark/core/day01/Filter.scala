package com.atguigu.spark.core.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author atguigu
  * Date 2020/1/6 11:43
  */
object Filter {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Filter").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20, true, "abc")
        val rdd1: RDD[Any] = sc.parallelize(list1, 2)
        
        //        val rdd2: RDD[Int] = rdd1.filter(x => x > 30)
        //        val rdd2: RDD[Int] = rdd1.filter(_ > 30)
        //        rdd1.filter(_> 30).map(_ + 10)
        /*val rdd2 = rdd1.collect {  // filter + map
            case x: Int if x > 30 => x + 10
        }*/
    
        val rdd2 = rdd1.collect {  // filter + map
            case x: Int if x > 30 => x + 10
        }
        rdd2.collect.foreach(println)
        sc.stop()
        
    }
}
