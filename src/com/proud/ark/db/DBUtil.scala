package com.proud.ark.db

import org.apache.spark.sql.SparkSession
import java.util.Properties
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SaveMode
import com.proud.ark.config.GlobalVariables
import java.sql.Connection
import java.sql.DriverManager
import org.apache.spark.sql.SQLContext
import com.proud.ark.data.HDFSUtil
import java.sql.ResultSet

/**
 * 一些从数据库中读取数据的模板方法，以及一些对数据库的数据预处理过，直接读取的操作。
 */

object DBUtil {
  val dbUrl = "jdbc:mysql://192.168.1.207:3306/test"
//	val dbUrl = "jdbc:mysql://192.168.0.14:3306/test"
  
  //  val crawlerDBUrl = "jdbc:mysql://180.76.189.148:3306/crawler?autoReconnect=true&failOverReadOnly=false&maxReconnects=10&interactiveClient=true&netTimeoutForStreamingResults=1800"
  val crawlerDBUrl = "jdbc:mysql://192.168.0.10/crawler?autoReconnect=true&failOverReadOnly=false&maxReconnects=10&interactiveClient=true&netTimeoutForStreamingResults=1800"
  
  val dbUrlIC = "jdbc:mysql://192.168.1.207:3307/test"
  
  private val user = "root"
  private val password = "PS@Letmein123"
//  private val user = "cron"
//  private val password = "arkcron"
  
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
	  
	  properties
  }
  
  def getCrawlerProperties(): Properties = {
	  var properties = new Properties()
	  properties.put("user", user)
//	  properties.put("password", "root")
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
  
  val jdbcName = "com.mysql.jdbc.Driver"
  
  val prop207 = getProperties()
  
  val propCrawler = getCrawlerProperties
  
  //默认操作的是207数据库
  def loadDFFromTable(table: String, spark:SparkSession) = {
    val df = spark.read.jdbc(dbUrl, table, prop207)
    df
  }
  
  //使用sqlContext
  def loadDFFromTable(table: String, sqlContext:SQLContext) = {
    val df = sqlContext.read.jdbc(dbUrl, table, prop207)
    df
  }
  
  def loadDFFromICTable(table: String, spark:SparkSession) = {
    val df = spark.read.jdbc(dbUrlIC, table, prop207)
    df
  }
  
  def loadDFFromTable(table: String, spark:SparkSession, predicates:Array[String]) = {
    val df = spark.read.jdbc(dbUrl, table, predicates, prop207)
    df
  }
  
  def loadDFFromTable(table: String, parallel:Int, spark:SparkSession) = {
    val (max, min) = getIdRange(table)
    val df = spark.read.jdbc(dbUrl, table, "id", min, max, parallel, prop207)
    df
  }
  
  def loadDFFromICTable(table: String, parallel:Int, spark:SparkSession) = {
    val (max, min) = getIdRange(table)
    val df = spark.read.jdbc(dbUrlIC, table, "id", min, max, parallel, prop207)
    df
  }
  
  
  def loadDFFromTableWithPredicate(table: String, batch:Int, spark:SparkSession, size:Long=500000) = {
    val first = (batch-1)*size+1
    val last = batch*size
    val predicates = Array[String](s"id between $first and $last")
    val df = spark.read.jdbc(dbUrl, table, predicates, prop207)
    df
  }
  
  def saveDFToDB[T](ds:Dataset[T], table:String, mode:SaveMode = SaveMode.Append, dbUrl:String = dbUrl, prefix:String = "") = {
    ds.write.mode(mode).jdbc(dbUrl, prefix+table, prop207)
  }
  
  def saveDFToICDB[T](ds:Dataset[T], table:String, mode:SaveMode = SaveMode.Append, dbUrl:String = dbUrl, prefix:String = "") = {
    ds.write.mode(mode).jdbc(dbUrlIC, prefix+table, prop207)
  }
  
  //在生成mysql语句时，使用replace into，而不是使用insert into
  def replaceDFToDB[T](ds:Dataset[T], table:String, mode:SaveMode = SaveMode.Append, dbUrl:String = dbUrl) = {
    saveDFToDB(ds, table, mode, dbUrl, "replace")
  }
  
  //在生成mysql语句时，使用insert ignore into，而不是使用insert into
  def insertIgnoreDFToDB[T](ds:Dataset[T], table:String, mode:SaveMode = SaveMode.Append, dbUrl:String = dbUrl) = {
    saveDFToDB(ds, table, mode, dbUrl, "ignore")
  }
  
  def insertOnUpdateDFToDB[T](ds:Dataset[T], table:String, mode:SaveMode = SaveMode.Append, dbUrl:String = dbUrl) = {
    saveDFToDB(ds, table, mode, dbUrl, "inonup")
  }
  
  def loadCrawlerData(table:String, spark:SparkSession) = {
    val df = spark.read.jdbc(crawlerDBUrl, table, propCrawler)
    df
  }
  
  def loadCrawlerData(table: String, parallel:Int, spark:SparkSession) = {
    val (max, min) = getCrawlerIdRange(table)
    val df = spark.read.jdbc(crawlerDBUrl, table, "id", min, max, parallel, propCrawler)
    df
  }
  
  def getConnection():Connection = {
    Class.forName(jdbcName)
    val connection = DriverManager.getConnection(dbUrl, user, password)
    connection
  }
  
  def getICConnection():Connection = {
    Class.forName(jdbcName)
    val connection = DriverManager.getConnection(dbUrlIC, user, password)
    connection
  }
  
  def getCrawlerConnection():Connection = {
    Class.forName(jdbcName)
    val connection = DriverManager.getConnection(crawlerDBUrl, user, password)
    connection
  }
  
  def truncate(table:String) = {
    val conn = getConnection
    try{
      val stmt = conn.prepareStatement(s"truncate ${table}")
      stmt.executeUpdate()
    }finally{
      conn.close()
    }
  }
  
  def truncateIC(table:String) = {
    val conn = getICConnection
    try{
      val stmt = conn.prepareStatement(s"truncate ${table}")
      stmt.executeUpdate()
    }finally{
      conn.close()
    }
  }
  
  def getIdRange(table:String):(Int, Int) = {
	  val conn = getConnection
    try{
      val stmt = conn.prepareStatement(s"select max(id), min(id) from ${table}")
      val rs = stmt.executeQuery()
      rs.next()
      (rs.getInt(1), rs.getInt(2))
    }finally{
      conn.close()
    }
  }
  
  def getCrawlerJobsUpdatedIds():Set[Int] = {
    val conn = getCrawlerConnection
    try{
    	val sql = "select id from jobs.job_info_monitor where type = 1"
      val stmt = conn.prepareStatement(sql)
      val rs = stmt.executeQuery()
      var ids = Set[Int]()
      while(rs.next()){
        ids = ids + (rs.getInt("id"))
      }
      ids
    }finally{
      conn.close()
    }
  }
  
  def getCrawlerCompanysUpdatedIds():Set[Int] = {
    val conn = getCrawlerConnection
    try{
    	val sql = "select id from jobs.job_company_monitor where type = 1"
      val stmt = conn.prepareStatement(sql)
      val rs = stmt.executeQuery()
      var ids = Set[Int]()
      while(rs.next()){
        ids = ids + (rs.getInt("id"))
      }
      ids
    }finally{
      conn.close()
    }
  }
  
  def executeUpdate(sql:String) = {
    val conn = getConnection
    try{
      val stmt = conn.prepareStatement(sql)
      stmt.executeUpdate()
    }finally{
      conn.close()
    }
  }
  
  
  def getCrawlerIdRange(table:String) = {
	  val conn = getCrawlerConnection()
    try{
      val stmt = conn.prepareStatement(s"select max(id), min(id) from ${table}")
      val rs = stmt.executeQuery()
      rs.next()
      (rs.getInt(1), rs.getInt(2))
    }finally{
      conn.close()
    }
  }
  
  def main(args: Array[String]): Unit = {
//    truncate("test.trigger_basic")
    println(getIdRange("dc_import.company"))
  }
  
}