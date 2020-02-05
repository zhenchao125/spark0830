package com.atguigu

/**
  * Author atguigu
  * Date 2020/2/5 11:32
  */
object MapDemo {
    def main(args: Array[String]): Unit = {
        val arr1 = Array(30, 50, 70, 60, 10, 20)
        val arr: Array[Int] = map(arr1, x => x * x * x)
        println(arr.mkString(", "))
    }
    
    
    def map(arr: Array[Int], op: Int => Int) = {
        for(e <- arr) yield op(e)
    }
}
