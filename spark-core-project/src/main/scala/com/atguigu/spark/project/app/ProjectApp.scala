package com.atguigu.spark.project.app

import com.atguigu.spark.project.bean.{CategoryCountInfo, UserVisitAction}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author atguigu
  * Date 2020/1/10 14:13
  */
object ProjectApp {
    def main(args: Array[String]): Unit = {
        // 1. 读取数据
        val conf: SparkConf = new SparkConf().setAppName("ProjectApp").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
//        sc.setLogLevel("error")
        val sourceRDD: RDD[String] = sc.textFile("C:/user_visit_action.txt")
        // 1.1 封装数据
        val userVisitActionRDD: RDD[UserVisitAction] = sourceRDD.map(line => {
            val splits: Array[String] = line.split("_")
            
            UserVisitAction(
                splits(0),
                splits(1).toLong,
                splits(2),
                splits(3).toLong,
                splits(4),
                splits(5),
                splits(6).toLong,
                splits(7).toLong,
                splits(8),
                splits(9),
                splits(10),
                splits(11),
                splits(12).toLong)
            
        })
        // 1.2 数据清洗
        
        // 2. 需求1:
        val categoryTop10: Array[CategoryCountInfo] = CategoryTopApp.statCategoryTop10(sc, userVisitActionRDD)
        // 3. 需求2:
        CategoryTop10SessionApp.calcCategorySessionTop10_3(sc, categoryTop10, userVisitActionRDD)
        
        // 4. 需求3:
        
        
        sc.stop()
    }
}
