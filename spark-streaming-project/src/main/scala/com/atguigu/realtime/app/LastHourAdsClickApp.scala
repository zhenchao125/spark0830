package com.atguigu.realtime.app
import com.atguigu.realtime.bean.AdsInfo
import org.apache.spark.streaming.dstream.DStream

/**
  * Author atguigu
  * Date 2020/1/15 16:56
  */
object LastHourAdsClickApp extends App {
    override def doSomething(ssc: DStream[AdsInfo]): Unit = {
    
    }
}
