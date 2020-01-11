import java.text.DecimalFormat

/**
  * Author atguigu
  * Date 2020/1/11 14:05
  */
object A {
    def main(args: Array[String]): Unit = {
        val formatter = new DecimalFormat(".00%")
        println(formatter.format(1.227222))
    }
}
