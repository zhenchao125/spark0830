package com.atguigu.spark.streaming.day02.withstate

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author atguigu
  * Date 2020/1/15 11:27
  */
object WordCount {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val ssc = new StreamingContext(conf, Seconds(5))
        ssc.checkpoint("./ck3")  // 用来保存状态
        val sourceStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)
        val wordAndOneStream: DStream[(String, Int)] = sourceStream.flatMap(_.split("\\W+")).map((_, 1))
        val wordCountStream = wordAndOneStream.updateStateByKey((seq: Seq[Int], opt: Option[Int]) => {
           Some(opt.getOrElse(0) + seq.sum)
        })
        wordCountStream.print()
        ssc.start()
        ssc.awaitTermination()
        
    }
}
