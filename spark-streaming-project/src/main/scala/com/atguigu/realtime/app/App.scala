package com.atguigu.realtime.app

import com.atguigu.realtime.bean.AdsInfo
import com.atguigu.realtime.util.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

trait App {
    def main(args: Array[String]): Unit = {
        // 1. 创建SteamingContext
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("App")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
        ssc.checkpoint("./realtime")
        // 2. 从kafka读数据  1579078203631,华东,上海,105,2  并封装到样例类中
        val sourceStream= MyKafkaUtil.getKafkaStream(ssc, "ads_log").map(s => {
            val split: Array[String] = s.split(",")
            AdsInfo(split(0).toLong, split(1), split(2), split(3), split(4))
        })
        
        // 3. 操作DStream
        doSomething(sourceStream)
        // 4. 启动ssc和阻止main方法退出
        ssc.start()
        ssc.awaitTermination()
    }
    
    def doSomething(ssc: DStream[AdsInfo]):Unit
    
}
