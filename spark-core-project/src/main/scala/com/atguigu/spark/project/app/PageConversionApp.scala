package com.atguigu.spark.project.app

import java.text.DecimalFormat

import com.atguigu.spark.project.bean.UserVisitAction
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Author atguigu
  * Date 2020/1/11 11:20
  */
object PageConversionApp {
    def statPageConversionRate(sc: SparkContext, userVisitActionRDD: RDD[UserVisitAction], pages: String) = {
        // "1,2,3,4,5,6,7"
        // 1. 得到目标页面
        val splits: Array[String] = pages.split(",")
        
        // 1.1 得到我们需要计算点击量的页面
        val prePages = splits.slice(0, splits.length - 1)
        val postPages = splits.slice(1, splits.length)
        // "1->2" "2->3"... "6->7"
        val pageFlow = prePages.zip(postPages).map {
            case (prePage, postPage) => s"$prePage->$postPage"
        }
        // 2. 计算目标的也没得点击量(分母)
        val pageAndCount: collection.Map[Long, Long] = userVisitActionRDD
            .filter(action => prePages.contains(action.page_id.toString))
            .map(action => (action.page_id, 1))
            .countByKey()
        
        // 3. 目标跳转流的数量(分子)
        val sessionIdGrouped: RDD[(String, Iterable[UserVisitAction])] = userVisitActionRDD.groupBy(_.session_id)
        val totalPageFlows = sessionIdGrouped.flatMap {
            case (sessionId, actionIt) =>
                // 先组装成我们跳转流  过过滤出来我们想要运算的跳转流
                val actions: List[UserVisitAction] = actionIt.toList.sortBy(_.action_time)
                val preActions: List[UserVisitAction] = actions.slice(0, actions.length - 1)
                val postActions: List[UserVisitAction] = actions.slice(1, actions.length)
                
                preActions.zip(postActions).map {
                    case (preAction, postAction) => s"${preAction.page_id}->${postAction.page_id}"
                }.filter(flow => pageFlow.contains(flow))
            // 计算这个session下的跳转流
        }.map((_, 1)).countByKey()
        
        
        // 4. 计算跳转率
        val result = totalPageFlows.map {
            // "1->2", 100      1
            case (pageFlow, count) =>
                val formatter = new DecimalFormat(".00%")
                val rate = count.toDouble / pageAndCount(pageFlow.split("->")(0).toLong)
                (pageFlow, formatter.format(rate))
        }
        result.foreach(println)
        
    }
}

/*
需要计算的转化率:  1,2,3,4,5,6,7   => 1->2  2->3, ....

分母: 1的点击量
    直接按照页面的id统计 reduceByKey  countByKey
分子: 1->2 的跳转流的数据
    1. 保证是同一个session   按照session分组
        按照时间排序

    2. RDD["1->2", "1->2",...]  map((_, 1)) reduceByKey

        RDD[1,3,4,1,2,5,6]  =>

        RDD[1,3,4,1,2,5]
        zip
        RDD[3,4,1,2,5,6]

        RDD[1->3, 3->4, 4->1, 1->2, 2->5, 5->6]
 */