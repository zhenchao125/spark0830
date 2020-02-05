

/**
  * Author atguigu
  * Date 2020/1/16 9:24
  */
object JsonTest {
    def main(args: Array[String]): Unit = {
        //        val list = List(("a",2), ("b",4))
        //        import org.json4s.JsonDSL._
        //        println(JsonMethods.compact(JsonMethods.render(list)))
        
        val list1 = List(40, 20, 70, 60, 10, 20)
//        list1.foreach(f)
        list1.foreach(x => {
            if (x <= 30) return
            println(x)
        })
        println("aaaa")
    }
    
    def f(x: Int):Unit = {
        
        if (x <= 30) return
        println(x)
        
    }
}


