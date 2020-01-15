package com.atguigu.mock.util

import scala.collection.mutable.ListBuffer

/**
  * 根据提供的值和比重, 来创建RandomOptions对象.
  * 然后可以通过getRandomOption来获取一个随机的预定义的值
  *
  * ('Lisi', 20),  ("zs", 10)
  */
object RandomOptions {
    def apply[T](opts: (T, Int)*): RandomOptions[T] = {
        val randomOptions = new RandomOptions[T]()
        randomOptions.totalWeight = (0 /: opts) (_ + _._2) // 计算出来总的比重
        opts.foreach {
            case (value, weight) => randomOptions.options ++= (1 to weight).map(_ => value)
        }
        randomOptions
    }
    
    
    def main(args: Array[String]): Unit = {
        // 测试
        val opts = RandomOptions(("张三", 10), ("李四", 30), ("ww", 20))
        
        println(opts.getRandomOption())
        println(opts.getRandomOption())
        println(opts.getRandomOption())
        println(opts.getRandomOption())
    }
}

// 工程师 10  程序猿 10  老师 20
class RandomOptions[T] {
    var totalWeight: Int = _
    var options = ListBuffer[T]()
    /**
      * 获取随机的 Option 的值
      *
      * @return
      */
    def getRandomOption() = {
        options(RandomNumUtil.randomInt(0, totalWeight - 1))
    }
}

