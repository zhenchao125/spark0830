package com.atguigu.spark.project.app

import com.atguigu.spark.project.bean.{CategoryCountInfo, UserVisitAction}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Author atguigu
  * Date 2020/1/10 16:12
  */
object CategoryTop10SessionApp {
    def calcCategorySessionTop10(sc: SparkContext, categoryTop10: Array[CategoryCountInfo], userVisitActionRDD: RDD[UserVisitAction]) = {
        // 1.过滤出来前10的品类id的点击记录\
        val cids: Array[Long] = categoryTop10.map(_.categoryId.toLong)
        val filteredUserVisitActionRDD: RDD[UserVisitAction] =
            userVisitActionRDD.filter(action => cids.contains(action.click_category_id))
        
        // 2. 每个品类的top10session(点击次数)
        val cidSessionAndOne: RDD[((Long, String), Int)] = filteredUserVisitActionRDD.map(action => ((action.click_category_id, action.session_id), 1))
        val cidAndSessionCount: RDD[(Long, (String, Int))] = cidSessionAndOne.reduceByKey(_ + _).map {
            case ((cid, sid), count) => (cid, (sid, count))
        }
        val cidAndSessionCountGrouped: RDD[(Long, Iterable[(String, Int)])] = cidAndSessionCount.groupByKey()
        
        val resultRDD: RDD[(Long, List[(String, Int)])] = cidAndSessionCountGrouped.mapValues(it => {
            it.toList.sortBy(-_._2).take(10)
        })
        
        resultRDD.collect.foreach(println)
        
    }
    
    def calcCategorySessionTop10_1(sc: SparkContext, categoryTop10: Array[CategoryCountInfo], userVisitActionRDD: RDD[UserVisitAction]) = {
        // 1.过滤出来前10的品类id的点击记录
        val cids: Array[Long] = categoryTop10.map(_.categoryId.toLong)
        val filteredUserVisitActionRDD: RDD[UserVisitAction] =
            userVisitActionRDD.filter(action => cids.contains(action.click_category_id))
        
        // 2. 每个品类的top10session(点击次数)
        
        for (cid <- cids) {
            val cidRDD: RDD[UserVisitAction] = filteredUserVisitActionRDD.filter(_.click_category_id == cid)
            val cidSidAndCount: RDD[((Long, String), Int)] = cidRDD
                .map(action => ((action.click_category_id, action.session_id), 1))
                .reduceByKey(_ + _)
            
            val result = cidSidAndCount
                .sortBy(-_._2)
                .map {
                    case ((cid, sid), count) => (cid, (sid, count))
                }
                .take(10)  // 行动算子
                .groupBy(_._1)
                .map {
                    case (cid, arr) => (cid, arr.map(_._2).toList)
                }
            println(result.toList)
            
        }
        
        
    }
}

/*
问题: 内存溢出

解决方案1:
    使用spark的排序  rdd.sortBy  sortByKey
    问题: 针对整个rdd排序
    
        每个cid取10个
        如果RDD只有一个cid 排序取前10
        
    好处: 不会内存溢出, 任务一定能跑下来
    缺点: 每个cid都会起一个job, 对资源的消耗比较大



1.过滤出来前10的品类id的点击记录

2. 每个品类的top10session(点击次数)

    ...
=> RDD[(cid, session), 1)] reduceByKey
=> RDD[(cid, session), count)] mpa
=> RDD[(cid, (session, count))] groupByKey
=> RDD[(cid, Iterable[(session, count)])]  map  每个iterator排序, 取前10




 */