package com.proud.ark.db

import java.util.Properties

/**
 * 生产数据库的数据库工具类。IP地址：180.76.176.235  内网地址：192.168.0.14
 */

object ProdIntranetDBUtil extends DBTrait {
  val dbUrl:String = "jdbc:mysql://192.168.0.14/jobs?autoReconnect=true&interactiveClient=true"
  
  val user:String = "root"
  
  val password:String = "root"
  
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