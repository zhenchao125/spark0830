package com.atguigu.spark.streaming.day02.kafka

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author atguigu
  * Date 2020/1/15 8:21
  */
object WordCount {
    def createSSC() = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val ssc = new StreamingContext(conf, Seconds(3))
        ssc.checkpoint("./ck1")
        val params: Map[String, String] = Map[String, String](
            "group.id" -> "0830",
            "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092")
        // 从kafka读取数据
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
            ssc,
            params,
            Set("s0830"))
            .flatMap {
                case (_, v) => v.split("\\W+").map((_, 1))
            }
            .reduceByKey(_ + _)
            .print()
        ssc
    }
    
    
    def main(args: Array[String]): Unit = {
        
        val ssc = StreamingContext.getActiveOrCreate("./ck1", createSSC)
        
        ssc.start()
        ssc.awaitTermination()
        
        
    }
}
