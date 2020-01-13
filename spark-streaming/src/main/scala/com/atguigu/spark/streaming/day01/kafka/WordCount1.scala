package com.atguigu.spark.streaming.day01.kafka


import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author atguigu
  * Date 2020/1/13 16:40
  */
object WordCount1 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("a").setMaster("local[2]")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))
        val params: Map[String, String] = Map[String, String](
            "group.id" -> "0830",
            "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092")
        // 从kafka读取数据
        val sourceStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
            ssc,
            params,
            Set("s0830"))
        sourceStream
            .map {
                case (_, v) => v
            }
            .flatMap(_.split("\\W+"))
            .map((_, 1))
            .reduceByKey(_ + _)
            .print(1000)
        
        
        
        ssc.start()
        
        ssc.awaitTermination()
    }
}
