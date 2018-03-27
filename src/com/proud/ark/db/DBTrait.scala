package com.proud.ark.db

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import java.util.Properties
import java.sql.Connection
import java.sql.DriverManager
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.Dataset

/**
 * 数据库工具的接口，定义了spark中涉及的基本的数据操作。子类只需要在此基础上提供其自定义的方法即可。
 * 此外，还需要提供其个性化的参数。
 */

trait DBTrait {
  
  val dbUrl:String
  
  val prop:Properties
  
  val user:String
  
  val password:String
  
  val jdbcName = "com.mysql.jdbc.Driver"
  
  def getProperties(): Properties
  
  def loadDFFromTable(table:String, spark:SparkSession) = {
    val df = spark.read.jdbc(dbUrl, table, prop)
    df
  }
  
  def loadDFFromTableRange(table:String, spark:SparkSession, min:Long = 1, max:Long = 1000) = {
    val df = spark.read.jdbc(dbUrl, table, "id", min, max, 1, prop)
    df
  }
  
  def loadDFFromTable(table: String, sqlContext:SQLContext) = {
    val df = sqlContext.read.jdbc(dbUrl, table, prop)
    df
  }
  
  def loadDFFromTable(table: String, spark:SparkSession, predicates:Array[String]) = {
    val df = spark.read.jdbc(dbUrl, table, predicates, prop)
    df
  }
  
  def loadDFFromTable(table: String, parallel:Int, spark:SparkSession) = {
	  val (max, min) = getIdRange(table)
    val df = spark.read.jdbc(dbUrl, table, "id", min, max, parallel, prop)
    df
  }
  
  def loadSampleDFFromTable(table: String, parallel:Int = 1, spark:SparkSession, min:Int = 1, max:Int = 10000) = {
    val df = spark.read.jdbc(dbUrl, table, "id", min, max, parallel, prop)
    df
  }
  
  def loadDFFromTable(table: String, parallel:Int, times:Int, currTime:Int, spark:SparkSession) = {
	  val (max, min) = getIdRange(table)
	  
	  val batch = (max-min)/times
	  val first = (currTime-1)*batch+1
	  val last = if(currTime == times) max else first+batch
    
	  val df = spark.read.jdbc(dbUrl, table, "id", min, max, parallel, prop)
    df
  }
  
  def saveDFToDB[T](ds:Dataset[T], table:String, mode:SaveMode = SaveMode.Append, prefix:String = "") = {
    ds.write.mode(mode).jdbc(dbUrl, prefix+table, prop)
  }
  
  
    //在生成mysql语句时，使用replace into，而不是使用insert into
  def replaceDFToDB[T](ds:Dataset[T], table:String, mode:SaveMode = SaveMode.Append) = {
    saveDFToDB(ds, table, mode, "replace")
  }
  
  //在生成mysql语句时，使用insert ignore into，而不是使用insert into
  def insertIgnoreDFToDB[T](ds:Dataset[T], table:String, mode:SaveMode = SaveMode.Append) = {
    saveDFToDB(ds, table, mode, "ignore")
  }
  
  def insertOnUpdateDFToDB[T](ds:Dataset[T], table:String, mode:SaveMode = SaveMode.Append) = {
    saveDFToDB(ds, table, mode, "inonup")
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
  
  def getConnection():Connection = {
    Class.forName(jdbcName)
    val connection = DriverManager.getConnection(dbUrl, user, password)
    connection
  }
}