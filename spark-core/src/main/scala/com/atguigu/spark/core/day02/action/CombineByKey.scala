package com.atguigu.spark.core.day02.action

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author atguigu
  * Date 2020/1/7 11:25
  */
object CombineByKey {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("FoldByKey").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val rdd = sc.parallelize(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)
//        val rdd2 = rdd.combineByKey(x => x, (_: Int) + (_: Int), (_: Int) + (_: Int))
//        val rdd2 = rdd.combineByKey(x => x, (_:Int) + _, (_:Int) + (_:Int))
        val rdd2 = rdd.combineByKey(x => x, (_:Int).max(_:Int), (_:Int) + (_:Int))
        
        rdd2.collect.foreach(println)
        sc.stop
    }
}

/*

聚合算子的特征:
 1. 都需要shuffle
 2. 都有预聚合

reduceByKey
    combineByKeyWithClassTag[V]((v: V) => v, func, func, partitioner)

foldByKey
    combineByKeyWithClassTag[V]((v: V) => cleanedFunc(createZero(), v),
      cleanedFunc, cleanedFunc, partitioner)

aggregateByKey
    combineByKeyWithClassTag[U]((v: V) => cleanedSeqOp(createZero(), v),
      cleanedSeqOp, combOp, partitioner)

combineByKey
    combineByKeyWithClassTag(createCombiner, mergeValue, mergeCombiners)(null)


 */