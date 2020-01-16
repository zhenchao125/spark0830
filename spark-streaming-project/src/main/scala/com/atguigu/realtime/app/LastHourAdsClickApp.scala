package com.atguigu.realtime.app

import com.atguigu.realtime.bean.AdsInfo
import com.atguigu.realtime.util.RedisUtil
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

/**
  * Author atguigu
  * Date 2020/1/15 16:56
  */
object LastHourAdsClickApp extends App {
    override def doSomething(adsInfoStream: DStream[AdsInfo]): Unit = {
        // 0. 先把窗口划分好
        val adsInfoSteamWithWindow: DStream[AdsInfo] = adsInfoStream.window(Minutes(60))
        // 1. 跳转数据类型((ads, hm), 1)
        val adsHMAndOne: DStream[((String, String), Int)] = adsInfoSteamWithWindow.map(info => ((info.adsId, info.hmString), 1))
        // 2. 聚合
        val adsHMAndCount: DStream[(String, Iterable[(String, Int)])] = adsHMAndOne
            .reduceByKey(_ + _)
            .map {
                case ((adsId, hm), count) => (adsId, (hm, count))
            }
            .groupByKey()
        
        // 3. 写入到redis
        adsHMAndCount.foreachRDD(rdd => {
            import org.json4s.JsonDSL._
            // 很多的隐式转换, 用来在java的集合和scala的集合中进行转换
            import scala.collection.JavaConversions._
            rdd.foreachPartition((it: Iterator[(String, Iterable[(String, Int)])]) => {
                val list: List[(String, Iterable[(String, Int)])] = it.toList
                if(list.size > 0){
                    val client: Jedis = RedisUtil.getJedisClient
                    val key = "last:ads:hour"
                    val map: Map[String, String] = list.toMap.map {
                        case (adsId, iter) =>
                            (adsId, JsonMethods.compact(JsonMethods.render(iter)))
                    }
                    println(map)
                    // 批处理的方式
                    client.hmset(key, map)
                    client.close()
                }
            })
        })
        
    }
}

/*
统计各广告最近 1 小时内的点击量趋势：各广告最近 1 小时内各分钟的点击量

每广告(分组)
, 最近1小时(window 窗口的长度)
, 每分钟(分组)的点击量
, 6秒更新一次(窗口滑动步长)

0. 先把窗口划分好

1. 跳转数据类型((ads, hm), 1)
2. 统计

3. 写入到redis
     /*
                     key                            value
                     "last:ads:hour"                hash
                                                    field     value
                                                    adsId   {"11:16": 100, "11:17": 2000, .....}
     */

 */