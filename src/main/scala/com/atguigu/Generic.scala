package com.atguigu

/**
  * Author atguigu
  * Date 2020/2/5 14:41
  */
object Generic {
    def main(args: Array[String]): Unit = {
        val bb = new BB
        val aa: AA = bb  // 多态
        
        // List是协变的
        val bbs: List[BB] = List[BB](new BB)
        var aas: List[AA] = bbs
        // 反过来就是逆变
        
        
    }
    
}

class AA
class BB extends AA
