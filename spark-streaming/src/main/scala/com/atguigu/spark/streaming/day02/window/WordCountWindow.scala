package com.atguigu.spark.streaming.day02.window

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author atguigu
  * Date 2020/1/15 14:01
  */
object WordCountWindow {
    def main(args: Array[String]): Unit = {
        // 统计最近15秒内的单词的次数, 每5秒统计一次
        
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Transform")
        val ssc = new StreamingContext(conf, Seconds(3))
        ssc.checkpoint("./ck6")
        val sourceStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)
        // 窗口有两个概念:  窗口的长度, 窗口的滑动步长  必须是时间间隔的整数倍
        sourceStream.flatMap(_.split("\\W+")).map((_, 1))
            //            .reduceByKeyAndWindow(_ + _, Seconds(15), slideDuration = Seconds(10)).print(10000)
//            .reduceByKeyAndWindow(_ + _, (n, l) => n - l, Seconds(15)).print(10000)
            .reduceByKeyAndWindow(_ + _, _ - _, Seconds(15), filterFunc = kv => kv._2 > 0).print(10000)
        
        ssc.start()
        ssc.awaitTermination()
        
    }
}
