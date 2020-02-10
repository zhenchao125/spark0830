package com.atguigu


/**
  * Author atguigu
  * Date 2020/2/5 11:54
  */
object CaseDemo {
    def main(args: Array[String]): Unit = {
        /*val Aaaaa = 20
        
        10 match {
            // 如果是小写字母开头, 表示新定义变量
            // 如果是大写字母开头, 则去找一个已经定义的常量
            case Aaaaa => println(Aaaaa)
        }*/
        
//        val a: Any = 10
        val a: Any = Array[Double](10, 30)
//        val a: Any = List[Double](10.1, 20.1)
        a match {
            // 匹配类型
            case b: Int =>
            // new int[]...
            case arr: Array[Int] => println("Array[Int]")
            case arr: List[_] => println("List[_]")
            case Array(a, 20) => println(a)
            
        }
        
        
        val arr2 = Array(30, 50, 70, 60, 10, 20)
    
        val Array(a1, b1, r@_*) = arr2
        println(a1)
        
        
        
        
    }
}
case class A(a: Int)
/*
泛型擦除
    编译成字节码之后, 泛型是不存在的
    
scala的数组: 底层其实就是java的数组
 */