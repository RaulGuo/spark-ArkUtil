package com.proud.ark.data

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.hadoop.conf.Configuration
import com.proud.ark.config.GlobalVariables

object HDFSUtil {
  
  val hdfsUrl = "hdfs://bigdata01:8020"
  
  def saveDSToHDFS[T](dir:String, ds:Dataset[T], mode:SaveMode = SaveMode.Append, hdfsUrl:String = hdfsUrl){
    ds.write.mode(mode).save(hdfsUrl+dir)
  }
  
  def getDSFromHDFS(dir:String, spark:SparkSession, hdfsUrl:String = hdfsUrl):Dataset[Row] = {
    spark.read.load(hdfsUrl+dir)
  }
  
  def loadCompanyNoGetiBasic(spark:SparkSession) = {
    getDSFromHDFS(GlobalVariables.hdfsCompanyDir, spark)
  }
  
  def removeDir(dir:String){
    import scala.sys.process._
    
    "hadoop fs -rm -r "+dir !
  }
}