package com.atguigu.sql.day02.udf

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, StructField, StructType}

import scala.collection.immutable.Nil

/**
  * Author atguigu
  * Date 2020/1/12 8:39
  */
object MySumDemo1 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
            .master("local[2]")
            .appName("MySumDemo1")
            .getOrCreate()
        val df: DataFrame = spark.read.json("c:/users.json")
        df.createOrReplaceTempView("user")
        
        // 先注册自定义的聚合函数
        spark.udf.register("my_sum", new MySum)
        spark.sql("select my_sum(age) from user").show
        
        spark.stop()
    }
}

/*
聚合函数
    用于弱类型   在sql语句中
        select my_sum(salary) from user group by name
    用于强类型   ds使用
 */

class MySum extends UserDefinedAggregateFunction {
    
    // 数据的数据类型
    override def inputSchema: StructType = StructType(StructField("c", DoubleType) :: Nil)
    
    // 缓冲区的类型
    override def bufferSchema: StructType = StructType(StructField("sum", DoubleType) :: Nil)
    
    // 最终聚合后的数据类型
    override def dataType: DataType = DoubleType
    
    // 相同输入是否应该有相同的输出
    override def deterministic: Boolean = true
    
    // 对缓冲区做初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer(0) = 0D
    }
    
    
    // 分区内的聚合
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        // 对传入数据做非空的判断
        if (!input.isNullAt(0)) {
            // 得到传给聚合函数的值
            val value: Double = input.getDouble(0)
            // 更新缓冲区
            buffer(0) = buffer.getDouble(0) + value
        }
    }
    
    
    // 分区间的聚合
    // 把buffer2的数据跟新到buffer1中
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        val value: Double = buffer2.getAs[Double](0)  // 等价于: buffer2.getDouble(0)
        buffer1(0) = buffer1.getDouble(0) + value
        
    }
    
    // 返回最后的聚合值
    override def evaluate(buffer: Row): Any = buffer(0)   // 等价于: buffer.getDouble(0)
}