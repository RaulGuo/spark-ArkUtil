package com.proud.ark.date

import java.text.SimpleDateFormat
import java.util.Date

object DateUtil {
  val sdf = new SimpleDateFormat("yyyy.MM.dd")
  
  //spark不支持存储java.util.Date, 需要转换为Java.sql.Date
  def transStringToDate(dateStr:String):java.sql.Date = {
    val date = sdf.parse(dateStr)
    new java.sql.Date(date.getTime)
  }
}