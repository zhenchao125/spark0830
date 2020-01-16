import org.json4s.jackson.JsonMethods

/**
  * Author atguigu
  * Date 2020/1/16 9:24
  */
object JsonTest {
    def main(args: Array[String]): Unit = {
        val list = List(("a",2), ("b",4))
        import org.json4s.JsonDSL._
        println(JsonMethods.compact(JsonMethods.render(list)))
    }
}
