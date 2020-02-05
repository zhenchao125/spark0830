package com.atguigu

import java.time.LocalDate

/**
  * Author atguigu
  * Date 2020/2/5 11:43
  */
object Demo5 {
//  implicit  def intToRichDate(day: Int) = new RichDate(day)
    
    
    def main(args: Array[String]): Unit = {
        val ago = "ago"
        val later = "later"
//        2.days(ago)
       val s =  2 days ago
       val s1 =  2 days later
        println(s)
        println(s1)
    }
    
    
    implicit class RichDate(day: Int){
        def days(when: String) = {
            val today: LocalDate = LocalDate.now()
            if("ago" == when){
                today.plusDays(-day).toString
            }else{
                today.plusDays(day).toString
            }
        }
    }
}


