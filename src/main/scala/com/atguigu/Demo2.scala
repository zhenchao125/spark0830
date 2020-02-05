package com.atguigu

object Demo2 {
    def main(args: Array[String]): Unit = {
        val arr1 = Array(30, 50, 70, 60, 10, 20)
        
//        arr1.foreach(x => println(x))  // 0
        arr1.foreach(println(_))  // 1
//
        arr1.foreach(println)  // 2  不是任何的简化
        
//        val aa: Any => Unit = println(_)
        // 我想有一个计算一个数的平方的函数
        val square: Double => Double = math.pow(2, _)
        println(square(3))
        println(square(4))
    }
}
/*
部分应用函数:
 
 */
