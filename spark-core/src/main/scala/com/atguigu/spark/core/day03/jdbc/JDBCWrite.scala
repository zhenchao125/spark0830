package com.atguigu.spark.core.day03.jdbc

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author atguigu
  * Date 2020/1/8 16:42
  */
object JDBCWrite {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("JDBCWrite").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        val list1 = List((30, "a"), (50, "b"), (70, "c"), (60, "d"))
        val rdd1 = sc.parallelize(list1, 2)
        
        
        //定义连接mysql的参数
        val driver = "com.mysql.jdbc.Driver"
        val url = "jdbc:mysql://hadoop102:3306/rdd"
        val userName = "root"
        val passWd = "aaaaaa"
        val sql = "insert into user1 values(?, ?)"
        /*rdd1.foreachPartition(it => {
            Class.forName(driver)
            val conn: Connection = DriverManager.getConnection(url, userName, passWd)
            
            it.foreach{
                case (age, name) =>
                    val ps: PreparedStatement = conn.prepareStatement(sql)
                    ps.setInt(1, age)
                    ps.setString(2, name)
                    ps.execute()
                    ps.close()
            }
            conn.close()
        })*/
        
        rdd1.foreachPartition(it => {
            Class.forName(driver)
            val conn: Connection = DriverManager.getConnection(url, userName, passWd)
            
            val ps: PreparedStatement = conn.prepareStatement(sql)
            it.foreach {
                case (age, name) =>
                    ps.setInt(1, age)
                    ps.setString(2, name)
                    ps.addBatch()
            }
            ps.executeBatch()
            conn.close()
        })
        
        sc.stop()
        
    }
}
