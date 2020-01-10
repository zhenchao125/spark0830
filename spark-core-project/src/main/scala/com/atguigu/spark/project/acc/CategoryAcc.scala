package com.atguigu.spark.project.acc

import com.atguigu.spark.project.bean.UserVisitAction
import org.apache.spark.util.AccumulatorV2

// Map[(cid, "click"), 1000]
// Map[(cid, "order"), 1000]
// Map[(cid, "pay"), 1000]
class CategoryAcc extends AccumulatorV2[UserVisitAction, Map[(String, String), Long]] {
    // 最终的返回结果
    private var map: Map[(String, String), Long] = Map[(String, String), Long]()
    
    override def isZero: Boolean = map.isEmpty
    //
    override def copy(): AccumulatorV2[UserVisitAction, Map[(String, String), Long]] = {
        val acc = new CategoryAcc
        acc.map ++= map
        acc
    }
    
    override def reset(): Unit = map = Map[(String, String), Long]()
    
    override def add(v: UserVisitAction): Unit = {
        // 分别累加三种操作
        if (v.click_category_id != -1) { // 表示点击事件
            // (click_category_id, "click") -> 1000
            map += (v.click_category_id.toString, "click") -> (map.getOrElse((v.click_category_id.toString, "click"), 0L) + 1L)
            
        } else if (v.order_category_ids != "null") { // 表示是下单事件   因为切割出来的字符串是 "null"
            val cids: Array[String] = v.order_category_ids.split(",")
            cids.foreach(cid => {
                map += (cid, "order") -> (map.getOrElse((cid, "order"), 0L) + 1L)
            })
            
        } else if (v.pay_category_ids != "null") { // 表示是支付事件
            val cids: Array[String] = v.pay_category_ids.split(",")
            cids.foreach(cid => {
                map += (cid, "pay") -> (map.getOrElse((cid, "pay"), 0L) + 1L)
            })
        }
    }
    
    override def merge(other: AccumulatorV2[UserVisitAction, Map[(String, String), Long]]): Unit = other match {
        case o: CategoryAcc =>
            // 合并两个map
            // 合并1:
            o.map.foreach {
                case (cidAction, count) =>
                    this.map += cidAction -> (this.map.getOrElse(cidAction, 0L) + count)
            }
            /*// 合并2:
            this.map = o.map.foldLeft(this.map) {
                case (map, (cidAction, count)) =>
                    map + (cidAction -> (this.map.getOrElse(cidAction, 0L) + count))
            }*/
        
        case _ => throw new UnsupportedOperationException
    }
    
    
    override def value: Map[(String, String), Long] = map
}
