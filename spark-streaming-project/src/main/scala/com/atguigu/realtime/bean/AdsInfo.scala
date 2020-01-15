package com.atguigu.realtime.bean

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

case class AdsInfo(ts: Long,
                   area: String,
                   city: String,
                   userId: String,
                   adsId: String,
                   var timestamp: Timestamp = null,
                   var dayString: String = null, // 2019-12-18
                   var hmString: String = null) { // 11:20
    timestamp = new Timestamp(ts)
    
    val date = new Date(ts)
    dayString = new SimpleDateFormat("yyyy-MM-dd").format(date)
    hmString = new SimpleDateFormat("HH:mm").format(date)
}

