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
      
        
    }
}




