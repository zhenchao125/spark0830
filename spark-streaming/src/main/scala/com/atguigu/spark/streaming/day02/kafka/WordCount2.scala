package com.atguigu.spark.streaming.day02.kafka

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaCluster.Err
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author atguigu
  * Date 2020/1/15 8:21
  */
object WordCount2 {
    val params: Map[String, String] = Map[String, String](
        "group.id" -> "0830",
        "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092")
    val topics = Set("s0830")
    val groupId = "bigdata"
    
    
    private val cluster = new KafkaCluster(params)
    
    /**
      * 读取offsets
      * 有可能是第一次启动, 也有可能不是
      *
      * KafkaUtils
      * KafkaCluster
      */
    def readOffsets() = {
        val topicAndPartitionEither: Either[Err, Set[TopicAndPartition]] = cluster.getPartitions(topics)
        
        var resultMap = Map[TopicAndPartition, Long]()
        topicAndPartitionEither match {
            case Right(topicAndPartitionSet) =>
                // 拿到了每个topic的每个分区的信息
                val topicAndPartitonAndOffsetsEither: Either[Err, Map[TopicAndPartition, Long]] = cluster.getConsumerOffsets(groupId, topicAndPartitionSet)
                topicAndPartitonAndOffsetsEither match {
                    // 表示不是第一次消费
                    case Right(map) =>
                        resultMap ++= map
                    // 表示是第一次消费. 把每个分区的offset设置为0
                    case _ =>
                        topicAndPartitionSet.foreach(topicAndPartition => {
                            resultMap += topicAndPartition -> 0L
                        })
                }
            
            case _ => // 如果是Left, 则啥都不需要处理. topic不存在
        }
        
        resultMap
    }
    
    /**
      * 每消费一次, 都应该保存消费记录
      */
    def saveOffsets(sourceStream: InputDStream[String]) = {
        sourceStream.foreachRDD(rdd => {  // 每个时间间隔内封装的那个rdd
            val hasOffsetRanges: HasOffsetRanges = rdd.asInstanceOf[HasOffsetRanges]
            
            val offsetRanges: Array[OffsetRange] = hasOffsetRanges.offsetRanges
            
            var map = Map[TopicAndPartition, Long]()
            offsetRanges.foreach(offsetRange => {
                val key: TopicAndPartition = offsetRange.topicAndPartition()
                val value: Long = offsetRange.untilOffset
                map += key -> value
            })
            
            // 保存消费记录
            cluster.setConsumerOffsets(groupId, map)
            
        })
    }
    
    def main(args: Array[String]): Unit = {
        
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val ssc = new StreamingContext(conf, Seconds(3))
        ssc.checkpoint("./ck1")
        
        // 可以手动维护 offset  使用低阶api
        val sourceStream: InputDStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](
            ssc,
            params,
            readOffsets(),
            (handler: MessageAndMetadata[String, String]) => handler.message()
        )
        
        sourceStream
            .flatMap(_.split("\\W+"))
            .map((_, 1))
            .reduceByKey(_ + _)
            .print(10000)
        
        saveOffsets(sourceStream)
        ssc.start()
        ssc.awaitTermination()
        
        
    }
    
}
