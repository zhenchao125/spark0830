package com.atguigu.sql.day02.udf

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator

/**
  * Author atguigu
  * Date 2020/1/12 10:15
  */
object MySumDemo2 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder()
            .master("local[2]")
            .appName("MySumDemo1")
            .getOrCreate()
        import spark.implicits._
        val df: DataFrame = spark.read.json("c:/users.json").selectExpr("cast(age as integer)", "name")
        
        val userDS: Dataset[User] = df.as[User].filter(_.age != null)
        //        val c: TypedColumn[User, Double] = new AVG().toColumn.name("my_avg")
        val c: TypedColumn[User, Double] = AVG.toColumn.name("my_avg")
        val ds: Dataset[Double] = userDS.select(c)
        ds.show()
        
        spark.stop()
    }
}

case class User(age: Integer, name: String)

case class AgeAvg(ageSum: Int, ageCount: Int) {
    def ageAvg = ageSum.toDouble / ageCount
}

/*
强类型的聚合函数
 
 
 */
object AVG extends Aggregator[User, AgeAvg, Double] {
    // 对缓冲区左初始化
    override def zero: AgeAvg = AgeAvg(0, 0)
    
    // 分区内的聚合
    override def reduce(b: AgeAvg, a: User): AgeAvg = {
        println(a)
        a match {
            case User(age, name) if age == null => b
            // 如果传入的不是null, 则进行聚合
            case u: User => AgeAvg(b.ageSum + u.age, b.ageCount + 1)
            // 如果是null, 则直接返回对象
        }
    }
    
    // 分区间的聚合
    override def merge(b1: AgeAvg, b2: AgeAvg): AgeAvg = {
        AgeAvg(b1.ageSum + b2.ageSum, b1.ageCount + b2.ageCount)
    }
    
    // 返回最终的聚合的值
    override def finish(reduction: AgeAvg): Double = reduction.ageAvg
    
    // 缓冲的编码器
    override def bufferEncoder: Encoder[AgeAvg] = Encoders.product // 如果是样例类(元组),在一定是这个
    
    // 输出的编码器
    override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
