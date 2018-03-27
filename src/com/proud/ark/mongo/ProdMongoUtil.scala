package com.proud.ark.mongo

import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.SparkSession
import com.mongodb.spark._
import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{max, min}
import org.bson.Document
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row


object ProdMongoUtil {
  
  def getBusinessBaseInfo(spark:SparkSession, province:String):Dataset[Row] = {
    
    val readConfig = ReadConfig(Map("uri" -> "mongodb://crawler:ark#2017@180.76.190.74:20001/gsxt", "collection" -> ("business_"+province)))
    
    val result = spark.read.mongo(readConfig).filter{
      x => {
        val tp = x.getAs[String]("type")
        tp == null || !tp.startsWith("个体")
      }
    }.limit(100)
    
    result
  }
  
}