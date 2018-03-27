package com.proud.ark.db

import java.util.Properties

object Prod153IntraDBUtil extends DBTrait{
  val dbUrl:String = "jdbc:mysql://192.168.0.24/ent?autoReconnect=true&interactiveClient=true"
  
  val user:String = "root"
  
  val password:String = "PS@Letmein123"
  
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
//	  properties.put("useUnicode", "true")
//	  properties.put("characterEncoding", "utf8md4")
	  properties.put("failOverReadOnly", "false")
	  properties.put("useSSL", "false")
	  
	  properties
  }
  
  val prop: Properties = getProperties()
}