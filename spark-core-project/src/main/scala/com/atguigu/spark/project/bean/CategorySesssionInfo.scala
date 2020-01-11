package com.atguigu.spark.project.bean

case class SessionInfo(sessionId: String,
                       count: Long) extends Ordered[SessionInfo]{
    override def compare(that: SessionInfo): Int = if(this.count <= that.count) 1 else -1
}