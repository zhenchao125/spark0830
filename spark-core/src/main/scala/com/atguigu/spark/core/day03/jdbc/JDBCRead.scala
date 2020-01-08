package com.atguigu.spark.core.day03.jdbc

import java.sql.{DriverManager, ResultSet}

import org.apache
import org.apache.spark
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext, rdd}

/**
  * Author atguigu
  * Date 2020/1/8 16:28
  */
object JDBCRead {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("JDBCRead").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
    
        //定义连接mysql的参数
        val driver = "com.mysql.jdbc.Driver"
        val url = "jdbc:mysql://hadoop102:3306/rdd"
        val userName = "root"
        val passWd = "aaaaaa"
    
    
        val rdd: RDD[(Int, String)] =  new JdbcRDD(
            sc,
            () => {
                Class.forName(driver)
                DriverManager.getConnection(url, userName, passWd)
            },
            "select age, name from user1 where age >= ? and age <= ?",
            10,
            50,
            2,
            (resultSet: ResultSet) => {
                (resultSet.getInt(1), resultSet.getString(2))
            }
        )
        
        rdd.collect().foreach(println)
        
        sc.stop()
        
        
        
    }
}
