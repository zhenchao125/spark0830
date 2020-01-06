package com.atguigu.spark.core.day01.singlevalue

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author atguigu
  * Date 2020/1/6 14:35
  */
object Distinct2 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Distinct").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        
        val list = List(new User(10, "b"), new User(10, "a"),new  User(15, "c"), new User(15, "d"))
        
        val rdd2 = sc.parallelize(list).distinct
        
        rdd2.collect.foreach(println)
        sc.stop()
        
    }
}


case class User(age: Int, name: String){
    override def hashCode(): Int = age
    
    override def equals(obj: Any): Boolean = {
        obj match {
            case User(age, name) => this.age == age
        }
    }
}
/*
1. 先看hashcode
2. 再是否为同一个对象
3. 然后再看equals
 */