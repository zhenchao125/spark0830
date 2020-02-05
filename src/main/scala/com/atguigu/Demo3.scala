package com.atguigu

/**
  * Author atguigu
  * Date 2020/2/5 10:33
  */
object Demo3 {
    def main(args: Array[String]): Unit = {
        /*val f = () => {
            println("f...")
            100
        }
        
        foo(f())*/
        
        foo{
            println("aaaaa")
            
            100
        }
    }
    
    def foo(a: => Int) = {
        println(a)
        println(a)
        println(a)
        println(a)
        println(a)
        C.a
        
    }
}


abstract class A{
    val a = 10
    def b = 20
    
    var c :Int
}

class B extends A{
    override val a: Int = 20
    
//    override def b: Int = 30
    override val b = 30
    
    override var c = 30
}

object C extends B