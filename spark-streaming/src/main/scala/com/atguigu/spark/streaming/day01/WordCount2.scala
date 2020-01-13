package com.atguigu.spark.streaming.day01

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * Author atguigu
  * Date 2020/1/13 14:36
  */
object WordCount2 {
    def main(args: Array[String]): Unit = {
        // 1. 创建一个StreamingContext
        val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("WordCount")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
        
        val queue = mutable.Queue[RDD[Int]]()
        val rddStream: InputDStream[Int] = ssc.queueStream(queue, false)
        
        rddStream.reduce(_ + _).print(1000)
        ssc.start()
        
        while(true){
            val rdd = ssc.sparkContext.parallelize(1 to 100)
            queue.enqueue(rdd)  // queue += rdd
            
            Thread.sleep(2000)
            println(queue.length)
        }
        // 6. 阻止main函数退出
        ssc.awaitTermination() // 阻塞方法, 阻塞主线程
    }
}
