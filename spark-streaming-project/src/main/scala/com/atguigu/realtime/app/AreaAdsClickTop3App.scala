package com.atguigu.realtime.app

import com.atguigu.realtime.bean.AdsInfo
import com.atguigu.realtime.util.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

/**
  * Author atguigu
  * Date 2020/1/15 16:29
  */
object AreaAdsClickTop3App extends App {
    override def doSomething(adsInfoStream: DStream[AdsInfo]): Unit = {
        
        val preKey = "day:area:ads:"
        // 1. 每天每地区每广告的点击量
        val dayAreaAdsAndCountStream: DStream[((String, String, String), Int)] = adsInfoStream
            .map(info => ((info.dayString, info.area, info.adsId), 1))
            .updateStateByKey((seq: Seq[Int], opt: Option[Int]) => {
                Some(seq.sum + opt.getOrElse(0))
            })
        //  2. 每天每地区分组
        val dayAreaGroupedStream: DStream[((String, String), Iterable[(String, Int)])] = dayAreaAdsAndCountStream.map {
            case ((day, area, ads), count) => ((day, area), (ads, count))
        }.groupByKey
        // 3. 按照点击排序取前3
        val resultStream: DStream[((String, String), List[(String, Int)])] = dayAreaGroupedStream.map {
            case ((day, area), adsCountIt) =>
                ((day, area), adsCountIt.toList.sortBy(-_._2).take(3))
        }
        
        // 4. 把数据存储到redis中
        resultStream.foreachRDD(rdd => {
            rdd.foreachPartition((it: Iterator[((String, String), List[(String, Int)])]) => {
                // 建立到redis的连接
                val client: Jedis = RedisUtil.getJedisClient
                // 遍历it, 把数据写入到redis
                it.foreach{
                    case ((day, area), adsCountList) =>
                        // redis中的key
                        val key: String = preKey + day
                        // hash: field
                        val field: String = area
                        // hash: value adsCountList变成json字符串   json4s
                        import org.json4s.JsonDSL._
                        val value: String = JsonMethods.compact(JsonMethods.render(adsCountList))
                        
                        client.hset(key, field, value)
                }
                
                // 关闭到redis的连接
                client.close()
            })
        })
    }
}

/*
每天每地区热门广告 Top3

1. 每天每地区每广告的点击量

2. 每天每地区分组

3. 按照点击排序取前3

4. 存储到redis中
    redis数据类型:
        string
        set
        hash
        list
            ...
        -------------------------------------------
    ((2020-01-16,华南),List((2,24), (1,23), (3,20)))
    ((2020-01-16,华中),List((3,9), (5,8), (2,7)))
    ((2020-01-15,华中),List((2,5), (3,3), (5,3)))
    ((2020-01-16,华北),List((2,25), (3,16), (1,16)))
    ((2020-01-15,华东),List((4,16), (1,11), (3,10)))
    ((2020-01-15,华南),List((2,11), (4,10), (5,7)))
    ((2020-01-15,华北),List((3,10), (2,8), (5,8)))
    ((2020-01-16,华东),List((5,24), (3,24), (4,22)))
    
    
    key                             value
    "day:area:ads"+2020-01-16       hash
                                    field           value
                                    "华南"           {"2": 24, "1": 23,...}
 */