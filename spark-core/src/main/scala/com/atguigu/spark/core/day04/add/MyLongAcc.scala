package com.atguigu.spark.core.day04.add

import org.apache.spark.util.AccumulatorV2

class MyLongAcc extends AccumulatorV2[Long, Long]{
    
    private var sum = 0L
    // 判断零值
    override def isZero: Boolean = sum == 0
    
    // 复制累加器
    override def copy(): AccumulatorV2[Long, Long] = {
        // 复制当前的累加器
        val acc = new MyLongAcc
        acc.sum = this.sum
        acc
    }
    
    // 重置累加器
    override def reset(): Unit = sum = 0
    
    // 累加(分区内累加)
    override def add(v: Long): Unit = sum += v
    
    // 分区间的合并
    override def merge(other: AccumulatorV2[Long, Long]): Unit = other match {
        case o: MyLongAcc => this.sum += o.sum
        case _ => throw new IllegalStateException("非法状态异常")
    }
    
    // 返回最终的累加值
    override def value: Long = sum
}
