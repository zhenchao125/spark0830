package com.atguigu.spark.core.day04.hbase

import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.{JsonMethods, Serialization}

import scala.collection.mutable

/**
  * Author atguigu
  * Date 2020/1/10 8:58
  */
object HbaseRead {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("HbaseRead").setMaster("local[2]")
        val sc: SparkContext = new SparkContext(conf)
        
        val hbaseConf: Configuration = HBaseConfiguration.create()
        hbaseConf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104")
        hbaseConf.set(TableInputFormat.INPUT_TABLE, "student")
        
        val rdd: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
            hbaseConf,
            classOf[TableInputFormat],
            classOf[ImmutableBytesWritable],
            classOf[Result]
        )
        
        val rdd1 = rdd.map {
            case (iw, result) =>
                // { "rowkew": 10001, "apple": 11, "age:" 10 }
                val map: mutable.Map[String, Any] = mutable.Map[String, Any]()
                val rowKey = Bytes.toString(iw.get())
                map += "rowKey" -> rowKey
                
                import scala.collection.JavaConversions._
                val cells: util.List[Cell] = result.listCells()
                for (cell <- cells) {
                    val key = Bytes.toString(CellUtil.cloneQualifier(cell))
                    val value = Bytes.toString(CellUtil.cloneValue(cell))
                    map += key -> value
                }
                // 把map集合转成json字符串   json4s (json4scala)
                
                import org.json4s.DefaultFormats
                Serialization.write(map)(DefaultFormats)
        }
        
        rdd1.collect().foreach(println)
        sc.stop()
        
    }
}
