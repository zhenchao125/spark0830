package com.atguigu

/**
  * Author atguigu
  * Date 2020/2/5 11:17
  */
object Demo4 {
    def main(args: Array[String]): Unit = {
        val user = User(10) // == User.apply(10)
        
//        val age = user("age")  // == user.apply("age")
        // arr(0)
        val age = user(0)  // == user.apply("age")
        println(age)
    }
}

object User{
    def apply(a: Int) = new User(10)
}
class User(val age: Int){
    def apply(m:Int) = age
}
