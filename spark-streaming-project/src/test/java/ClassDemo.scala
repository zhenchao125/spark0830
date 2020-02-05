/**
  * Author atguigu
  * Date 2020/1/16 14:18
  */
object ClassDemo {
    def main(args: Array[String]): Unit = {
        
    }
}
abstract class A{
    val a = 1
    var b : Int
    
    def foo() = {
        println("foo..")
    }
}

class B extends A{
    override val a = 10
    override var b = 20
}