package com.atguigu.spark.streaming.day02.unstate

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author atguigu
  * Date 2020/1/15 11:12
  */
object Transform {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Transform")
        val ssc = new StreamingContext(conf, Seconds(3))
        
        
        val sourceStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)
        
        val result: DStream[(String, Int)] = sourceStream.transform(rdd => {
            rdd.flatMap(_.split("\\W+").map((_, 1))).reduceByKey(_ + _)
        })
        
        
        val spark: SparkSession = SparkSession
            .builder()
            .config(conf)
            .getOrCreate()
        import spark.implicits._
        val props = new Properties()
        props.put("user", "root")
        props.put("password", "aaaaaa")
        // 用于写入到外部存储
        result.foreachRDD(rdd => {
            /*rdd.foreachPartition(it => {
                //
            })*/
            val df: DataFrame = rdd.toDF("word", "count")
            df.write
                .mode("append")
                .jdbc("jdbc:mysql://hadoop102:3306/rdd", "word", props)
        })
        
//        result.print
        
        ssc.start()
        ssc.awaitTermination()
        
    }
}
