package com.atguigu.spark.core.day02.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author atguigu
  * Date 2020/1/7 15:11
  */
object Practice {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val lineRDD: RDD[String] = sc.textFile("c:/agent.log")
        // RDD[(省份, 广告), 1)]
        val proAdsAndOne: RDD[((String, String), Int)] = lineRDD.map(line => {
            val split: Array[String] = line.split(" ")
            ((split(1), split(4)), 1)
        })
        // RDD[(省份, 广告), count)]
        val proAdsAndCount: RDD[((String, String), Int)] = proAdsAndOne.reduceByKey(_ + _)
        // RDD[(省份, (广告, count))]  RDD[(省份, List((广告1, count1), (广告2, count2), (广告3, count3), ...)]
        val proAndAdsCountGrouped: RDD[(String, Iterable[(String, Int)])] = proAdsAndCount.map {
            case ((pro, ads), count) => (pro, (ads, count))
        }.groupByKey()
        // 排序, 取前3
        val resultRDD: RDD[(String, List[(String, Int)])] = proAndAdsCountGrouped.map {
            case (pro, it) =>
                (pro, it.toList.sortBy(_._2)(Ordering.Int.reverse).take(3))
        }.sortBy(_._1.toInt)
        
        resultRDD.collect.foreach(println)
        sc.stop()
        
    }
}

/*
1.	数据结构：时间戳，省份，城市，用户，广告，字段使用空格分割。
        1516609143867 6 7 64 16
        1516609143869 9 4 75 18
        1516609143869 1 7 87 12
2.	需求: 统计出每一个省份广告被点击次数的 TOP3

倒推法:
=> ...
=> RDD[(省份, 广告))]  map
=> RDD[(省份, 广告), 1)] reduceByKey
=> RDD[(省份, 广告), count)]  map
=> RDD[(省份, (广告, count))]    groupByKey
=> RDD[(省份, List((广告1, count1), (广告2, count2), (广告3, count3), ...)]  排序, 取前3
RDD[(省份, List((广告1, count1), (广告2, count2), (广告3, count3))]

 */