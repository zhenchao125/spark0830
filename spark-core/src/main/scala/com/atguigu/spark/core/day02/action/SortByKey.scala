package com.atguigu.spark.core.day02.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author atguigu
  * Date 2020/1/7 14:16
  */
object SortByKey {
    // Ordered
    implicit val ord:Ordering[User] = new Ordering[User]{
        override def compare(x: User, y: User): Int = x.age - y.age
    }
    
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("FoldByKey").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        //        val rdd = sc.parallelize(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)
        //        val rdd2: RDD[(String, Int)] = rdd.sortByKey(ascending = false)
        val rdd = sc.parallelize(User(10, "d") :: User(8, "e") :: User(12, "a") :: Nil)
        val rdd2: RDD[(User, Int)] = rdd.map((_, 1))
        val rdd3: RDD[(User, Int)] = rdd2.sortByKey(false)
        rdd3.collect.foreach(println)
        sc.stop
    }
}

case class User(age: Int, name: String)
