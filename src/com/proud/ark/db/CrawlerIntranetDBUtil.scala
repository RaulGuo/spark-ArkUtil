package com.proud.ark.db

import java.util.Properties

/**
 * 爬虫机器的DBUtil类。IP地址：180.76.189.148 内网地址：192.168.0.10
 */

object CrawlerIntranetDBUtil extends DBTrait {
  
  val dbUrl:String = "jdbc:mysql://192.168.0.10/crawler?autoReconnect=true&failOverReadOnly=false&maxReconnects=10&interactiveClient=true&netTimeoutForStreamingResults=1800"
  
  val user:String = "cron"
  
  val password:String = "arkcron"
  
  def getProperties(): Properties = {
	  var properties = new Properties()
	  properties.put("user", user)
	  properties.put("password", password)
	  properties.put("driver", "com.mysql.jdbc.Driver")
	  properties.put("useServerPrepStmts", "false")
	  properties.put("rewriteBatchedStatements","true")
	  properties.put("interactiveClient", "true")
	  properties.put("netTimeoutForStreamingResults", "1800")
	  properties.put("autoReconnect", "true")
	  properties.put("useUnicode", "true")
	  properties.put("characterEncoding", "utf-8")
	  properties.put("failOverReadOnly", "false")
	  
	  properties
  }

  val prop: Properties = getProperties()
}