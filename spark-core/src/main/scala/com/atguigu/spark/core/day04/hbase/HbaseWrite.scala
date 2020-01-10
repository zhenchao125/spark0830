package com.atguigu.spark.core.day04.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, rdd}

/**
  * Author atguigu
  * Date 2020/1/10 9:27
  */
object HbaseWrite {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
        val sc = new SparkContext(conf)
    
        val hbaseConf = HBaseConfiguration.create()
        hbaseConf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104")
        hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, "student")
        
        val job: Job = Job.getInstance(hbaseConf)
        job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
        job.setOutputKeyClass(classOf[ImmutableBytesWritable])
        job.setOutputValueClass(classOf[Put])
        
        
        // 先rdd封装成HBase需要的那种数据格式
        val initialRDD = sc.parallelize(List(("a", "apple", "11"), ("b", "banana", "12"), ("c", "pear", "13")))
        // (ImmutableBytesWritable, Put)
        val reslutRDD: RDD[(ImmutableBytesWritable, Put)] = initialRDD.map {
            case (row, name, age) =>
                val rowKey: ImmutableBytesWritable = new ImmutableBytesWritable()
                rowKey.set(Bytes.toBytes(row))
            
                val put = new Put(Bytes.toBytes(row))
                put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"), Bytes.toBytes(name))
                put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("age"), Bytes.toBytes(age))
                (rowKey, put)
        }
        
        reslutRDD.saveAsNewAPIHadoopDataset(job.getConfiguration)
        sc.stop()
    
    }
}
