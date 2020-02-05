package com.atguigu

import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.{A, B}

/**
  * Author atguigu
  * Date 2020/2/5 11:54
  */
object CaseDemo {
    def main(args: Array[String]): Unit = {
        val Aaaaa = 20
        
        10 match {
                // 如果是小写字母开头, 表示新定义变量
                // 如果是大写字母开头, 则去找一个已经定义的常量
            case Aaaaa => println(Aaaaa)
        }
    }
}
