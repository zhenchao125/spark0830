package com.atguigu.spark.core.day01.singlevalue

import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

/**
  * Author atguigu
  * Date 2020/1/6 15:23
  */
object SortBy1 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("SortBy").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List("hello", "abc", "aaa", "abcde")
        val rdd1 = sc.parallelize(list1, 2)
        
        //        val rdd2= rdd1.sortBy(x => x, ascending = true)  // 位置参数 命名参数
        //        val rdd2= rdd1.sortBy(_.length, false)  // 位置参数 命名参数
        //        val rdd2 = rdd1.sortBy(x => (x.length, x), false)
        // 先安装字符串的长度升序, 如果长度相等, 按照字典的降序
        val rdd2 = rdd1
            .sortBy(x => (x.length, x))(Ordering.Tuple2(Ordering.Int.reverse, Ordering.String), ClassTag(classOf[(Int, String)]))
        rdd2.collect.foreach(println)
        
        sc.stop()
        
        
    }
}
