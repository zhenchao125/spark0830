package com.atguigu.spark.streaming.day01

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author atguigu
  * Date 2020/1/13 14:36
  */
object WordCount {
    def main(args: Array[String]): Unit = {
        // 1. 创建一个StreamingContext
        val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("WordCount")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
        // 2. 从数据源得到 DSteam
        val sourceStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)
        
        // 3. 对流做各种转换
        val wordAndCount = sourceStream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
        // 4. 展示数据(行动算子)
        wordAndCount.print(1000)
        // 5. 启动流处理
        ssc.start()
        // 6. 阻止main函数退出
        ssc.awaitTermination()  // 阻塞方法, 阻塞主线程
    }
}
