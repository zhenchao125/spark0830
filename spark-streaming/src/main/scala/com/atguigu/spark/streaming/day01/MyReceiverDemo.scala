package com.atguigu.spark.streaming.day01

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author atguigu
  * Date 2020/1/13 16:06
  */
object MyReceiverDemo {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("a").setMaster("local[2]")
        val ssc = new StreamingContext(conf, Seconds(3))
        val lineStream: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver("hadoop102", 10000))
        
        lineStream.flatMap(_.split("\\W+")).map((_, 1)).reduceByKey(_ + _).print(1000)
        
        
        ssc.start()
        
        ssc.awaitTermination()
        
    }
}


/*
1. 接受socket的数据
 */
class MyReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {
    var socket: Socket = _
    var reader: BufferedReader = _
    
    // 当接收器启动的时候回调这个函数
    override def onStart(): Unit = {
        // 去创建一个socket
        runInThread {
            try {
                // 网络聊天室
                socket = new Socket(host, port)
                reader = new BufferedReader(new InputStreamReader(socket.getInputStream, "utf-8"))
                var line: String = reader.readLine()
                while (socket.isConnected && line != null) {
                    store(line)
                    line = reader.readLine()
                }
            } catch {
                case e => println(e.getMessage)
            } finally {
               
                restart("重启receiver....")
            }
        }
        
        // 从socket读取数据
        
        
        // 把数据发送到其他的executor上
    }
    
    // 接收器停止的时候回调这个函数: 释放资源
    override def onStop(): Unit = {
        if(socket != null){
            socket.close()
        }
        if(reader != null){
            reader.close()
        }
    }
    
    
    
    def runInThread(f: => Unit): Unit = {
        new Thread() {
            override def run(): Unit = f
        }.start()
    }
}