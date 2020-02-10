package com.atguigu

import scala.reflect.ClassTag

/**
  * Author atguigu
  * Date 2020/2/5 14:29
  */
object ClassTagDemo1 {
    def main(args: Array[String]): Unit = {
        val ints: Array[Int] = newArray[Int](10)
        val doubles: Array[Double] = newArray[Double](10)
    }
    // 运行的时候记住泛型的类型  ClassTag[T]
    /*def newArray[T: ClassTag](len: Int) = {
        new Array[T](len)  // new int[]...
    }*/
    
    def newArray[T](len: Int)(implicit ct: ClassTag[T]) = new Array[T](len)
    
}
