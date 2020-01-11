package com.atguigu.spark.project.app

import com.atguigu.spark.project.bean.{CategoryCountInfo, SessionInfo, UserVisitAction}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkContext}

import scala.collection.mutable

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
                .take(10) // 行动算子
                .groupBy(_._1)
                .map {
                    case (cid, arr) => (cid, arr.map(_._2).toList)
                }
            println(result.toList)
        }
        
        
    }
    
    def calcCategorySessionTop10_2(sc: SparkContext, categoryTop10: Array[CategoryCountInfo], userVisitActionRDD: RDD[UserVisitAction]) = {
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
        
        val resultRDD = cidAndSessionCountGrouped.mapValues(it => {
            // 创建一个Treeset来进行自动的排序, 每次只取前10个
            var set = new mutable.TreeSet[SessionInfo]()
            it.foreach {
                case (sessionId, count) =>
                    set += SessionInfo(sessionId, count)
                    if (set.size > 10) set = set.take(10)
            }
            set.toList
        })
        
        resultRDD.collect.foreach(println)
        
    }
    
    // 对calcCategorySessionTop10_2的改良, 减少一次分区:  在reduceByKey的时候, 保证一个分区内只有一个Category的信息
    def calcCategorySessionTop10_3(sc: SparkContext, categoryTop10: Array[CategoryCountInfo], userVisitActionRDD: RDD[UserVisitAction]) = {
        // 1.过滤出来前10的品类id的点击记录\
        val cids: Array[Long] = categoryTop10.map(_.categoryId.toLong)
        val filteredUserVisitActionRDD: RDD[UserVisitAction] =
            userVisitActionRDD.filter(action => cids.contains(action.click_category_id))
        
        // 2. 每个品类的top10session(点击次数)
        val cidSessionAndOne: RDD[((Long, String), Int)] = filteredUserVisitActionRDD.map(action => ((action.click_category_id, action.session_id), 1))
        // 聚合的时候, 让一个分区内只有一个Category, 则后续不再需要分组,提示性能
        val cidAndSessionCount: RDD[(Long, (String, Int))] = cidSessionAndOne.reduceByKey(new CategoryPartitioner(cids), _ + _).map {
            case ((cid, sid), count) => (cid, (sid, count))
        }
        
        
        val resultRDD = cidAndSessionCount.mapPartitions(it => {
            
            var set = new mutable.TreeSet[SessionInfo]()
            var categoryId = 0L
            it.foreach {
                case (cid, (sessionId, count)) =>
                    categoryId = cid
                    set += SessionInfo(sessionId, count)
                    if (set.size > 10) set = set.take(10)
            }
            set.map((categoryId, _)).toIterator
        })
        
        resultRDD.collect.foreach(println)
        
    }
    
}

class CategoryPartitioner(cids: Array[Long]) extends Partitioner {
    private val map: Map[Long, Int] = cids.zipWithIndex.toMap
    
    // 分区的个数设置为和品类的id树保持一致, 将来保证一个分区内只有一个品类的数据
    override def numPartitions: Int = cids.length
    
    // 根据key来返回这个key应用取的那个分区的索引
    override def getPartition(key: Any): Int = {
        key match {
            case (k: Long, _) => map(k)
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
解决方案2:




1.过滤出来前10的品类id的点击记录

2. 每个品类的top10session(点击次数)

    ...
=> RDD[(cid, session), 1)] reduceByKey
=> RDD[(cid, session), count)] mpa
=> RDD[(cid, (session, count))] groupByKey
=> RDD[(cid, Iterable[(session, count)])]  map  每个iterator排序, 取前10




 */