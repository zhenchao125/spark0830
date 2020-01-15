package com.atguigu.spark.streaming.day02.output

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author atguigu
  * Date 2020/1/15 14:38
  */
object Output1 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Transform")
        val ssc = new StreamingContext(conf, Seconds(3))
    
    
        val sourceStream: DStream[String] = ssc.socketTextStream("hadoop102", 9999)
        
        sourceStream
            .flatMap(_.split("\\W+"))
            .map((_, 1))
            .reduceByKey(_ + _)
            .saveAsTextFiles("log")
        
        ssc.start()
        ssc.awaitTermination()
    
    }
}
