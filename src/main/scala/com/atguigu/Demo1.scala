package com.atguigu

/**
  * Author atguigu
  * Date 2020/2/5 9:24
  */
object Demo1 {
    def main(args: Array[String]): Unit = {
        // ? :
        val m = 10
        val n = 100
        val max = if (m > n) m else n
        println(max)
        
        var a = 1
        
        val b = while (a < 3) {
            a += 1
        }
        println(b)
        
        var c = 1
        val d = c = 2
        
        
        // for 推导
        val arr = for (i <- 0 to 10) yield i * i
        println(arr)
        
    }
}

/*
循环(for while):  值 Unit
赋值语句:  值Unit
其他的情况: 最后一行代码
 */