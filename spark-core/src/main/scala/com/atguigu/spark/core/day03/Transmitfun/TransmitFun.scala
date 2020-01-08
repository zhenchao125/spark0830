package com.atguigu.spark.core.day03.Transmitfun


import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object TransmitFun {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf()
            .setAppName("SerDemo")
            .setMaster("local[*]")
            // set("spark.serializer", classOf[KryoSerializer].getName)  org.apache.spark.serializer.KryoSerializer
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")  // 多余
            .registerKryoClasses(Array(classOf[Searcher]))
        
        val sc = new SparkContext(conf)
        val rdd: RDD[String] = sc.parallelize(Array("hello world", "hello atguigu", "atguigu", "hahah"), 2)
        val searcher = new Searcher("hello") // 对象是在driver 创建
        val result: RDD[String] = searcher.getMatchedRDD1(rdd)
        result.collect.foreach(println)
        // kryo 通用的序列化机制
        new HashPartitioner(3)
    }
}

//需求: 在 RDD 中查找出来包含 query 子字符串的元素

// query 为需要查找的子字符串
class Searcher(val query: String) extends Serializable {
    
    // 判断 s 中是否包括子字符串 query
    def isMatch(s: String): Boolean = {
        s.contains(query)
        
    }
    
    // 过滤出包含 query字符串的字符串组成的新的 RDD  需要类实现Serializable
    def getMatchedRDD1(rdd: RDD[String]): RDD[String] = {
        rdd.filter(isMatch) //
    }
    
    // 需要类实现Serializable
    def getMatchedRDD2(rdd: RDD[String]): RDD[String] = {
        rdd.filter(s => s.contains(query))
    }
    
    // 不需要实现Serializable
    def getMatchedRDD3(rdd: RDD[String]): RDD[String] = {
        val q: String = this.query
        rdd.filter(s => s.contains(q))
    }
    
}

/*
在RDD的算子中用到对象的属性或方法的时候, 对象需要序列化

 `1. 传递方法
  2. 传递函数
  
如果是传递的局部遍历, 则不用考虑真个对象的序列化


 */
