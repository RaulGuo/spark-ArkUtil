package com.proud.ark.date

import java.text.SimpleDateFormat
import java.util.Date

object DateUtil {
  val sdf = new SimpleDateFormat("yyyy.MM.dd")
  
  val standardDF = new SimpleDateFormat("yyyy-MM-dd")
  
  
  //spark不支持存储java.util.Date, 需要转换为Java.sql.Date
  def transStringToDate(dateStr:String):java.sql.Date = {
    if(dateStr == null || dateStr.trim().isEmpty || dateStr.trim().length() > 10) {
      return null;
    }
    
    try{
      val date = sdf.parse(dateStr)
      return new java.sql.Date(date.getTime)
    }catch{
      case e:Exception => return null;
    }
  }
  
  def transStringToStandard(dateStr:String):String = {
    if(dateStr == null || dateStr.trim().isEmpty || dateStr.trim().length() > 10 || dateStr.trim().length() < 8) {
      return null;
    }
    
    try{
//      val date = sdf.parse(dateStr)
      return dateStr.trim().replace(".", "-")
    }catch{
      case e:Exception => return null;
    }
  }
  
  
}