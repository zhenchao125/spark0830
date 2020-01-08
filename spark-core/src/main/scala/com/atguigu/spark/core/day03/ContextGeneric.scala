package com.atguigu.spark.core.day03

object ContextGeneric {
    def main(args: Array[String]): Unit = {
        //        println(max("a", "b")(Ordering.String))
        //        println(max(1, 2)(Ordering.Int))
        println(max(1, 2))
        println(max("a", "b"))
        // Ordering[User]
        implicit val ord: Ordering[User] = new Ordering[User]{
            override def compare(x: User, y: User): Int = x.age - y.age
        }
        println(max(User(10, "a"), User(20, "b")))
    }
    
    // 表示必须有一个隐式值   Ordering[T]
    def max[T: Ordering](a: T, b: T): T = {
        // 从冥界召唤隐式值
        val ord: Ordering[T] = implicitly[Ordering[T]]
        if (ord.gteq(a, b)) a else b
    }
    
    // AnyVal  AnyRef
    //    def max[T](a: T, b: T)(implicit ord: Ordering[T]): T ={
    //        if(ord.gteq(a, b)) a else b
    //    }
}

case class User(age:Int, name:String)

/*
[T: K]


 */
