package com.atguigu

/**
  * Author atguigu
  * Date 2020/2/5 14:18
  */
object CaseDemo2 {
    def main(args: Array[String]): Unit = {
        /*val arr1 = Array(30, 50, 70, 60, 10, 20).map((_, 1))
        
//        arr1.foreach(t => println(t._1))
        arr1.foreach{
            case (w, c) => println(w)
        }*/
        
        val arr1 = Array(1, 2, 3, "zs", true)
        
        /*val arr2 = arr1
            .filter(x => x.isInstanceOf[Int])
            .map(x => x.asInstanceOf[Int])
            .map(x => x * x)
        println(arr2.mkString(", "))*/
        val arr2 = arr1.collect {
            case a: Int => a * a
        }
        println(arr2.mkString(", "))
    }
}

/*
spark 1. collect
      2. 有和scala同样功能
 */