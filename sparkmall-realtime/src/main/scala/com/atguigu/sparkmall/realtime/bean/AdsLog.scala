package com.atguigu.sparkmall.realtime.bean

import java.text.SimpleDateFormat
import java.util.Date

case class AdsLog(ts: Long, area: String, city: String, userId: String, adsId: String) {

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")

    def getDate() = {
        dateFormat.format(new Date(ts))
    }
}
