package com.atguigu.spark.core.day03.partitioner

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
  * Author atguigu
  * Date 2020/1/8 14:12
  */
object MyPartitioner {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("MyPartitioner").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List(30, 50, 70, 60, 10, 20, null, null)
        val rdd1: RDD[Any] = sc.parallelize(list1, 4)
        
        val kvRDD: RDD[(Any, Int)] = rdd1.map((_, 1))
        val result: RDD[(Any, Int)] = kvRDD.partitionBy(new MyPartitioner(2)).reduceByKey(new MyPartitioner(3), _ + _)
        val rdd: RDD[Array[(Any, Int)]] = result.glom()
        rdd.collect().foreach(a => {
            println("a: " + a.mkString(", "))
        })
        
        Thread.sleep(10000000)
        sc.stop()
        
    }
}


class MyPartitioner(val partitionNum: Int) extends Partitioner{
    // 分区的个数
    override def numPartitions: Int = partitionNum
    
    // 根据key来计算这个k-v应该进入到那个分区中
    override def getPartition(key: Any): Int = key match {
        case null => 0
        case _ => key.hashCode().abs % partitionNum
//        case _ => 1
    }
    
    override def hashCode(): Int = partitionNum
    
    override def equals(obj: Any): Boolean = obj match {
        case p:MyPartitioner => p.partitionNum == partitionNum
        case _ => false
    }
}