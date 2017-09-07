package com.proud.ark.data

import com.proud.ark.db.DBUtil
import com.proud.ark.config.ConfigUtil
import org.apache.spark.sql.SparkSession
import com.proud.ark.config.GlobalVariables
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

/**
从数据库中读取企业数据，并保存到HDFS中。
并将企业数据中的个体户过滤掉，保存到HDFS中。
在运行之前要将相应的本地目录清空，否则append的方式会导致重复数据
nohup spark-submit --master local[*] --driver-memory 20g --class com.proud.ark.data.RefreshCompanyBasicData --jars /home/data_center/dependency/mysql-connector-java.jar /home/data_center/dependency/ArkUtil-0.0.1-SNAPSHOT.jar &
spark-submit --master spark://bigdata01:7077 --executor-memory 9g --class com.proud.ark.data.RefreshCompanyBasicData --jars /home/data_center/dependency/mysql-connector-java.jar /home/data_center/dependency/ArkUtil-0.0.1-SNAPSHOT.jar
 */

object RefreshCompanyBasicData {
  val spark = SparkSession.builder().appName("RefreshCompanyBasic").config("spark.sql.warehouse.dir", ConfigUtil.warehouse).config("master", ConfigUtil.master).getOrCreate()
  val sc = spark.sparkContext
  import spark.implicits._
  
  def main(args: Array[String]): Unit = {
    println("-------------------hello world---------------")
    if(args != null && args.length > 0){
      args.foreach( province => {
        val companyDF = DBUtil.loadDFFromTable("dc_import."+province+"_company", 16, spark).select("id", "type", "name", "md5")
        appendCompanyByProvince(companyDF)
      })
    } else {
      GlobalVariables.provinces.foreach { province => 
      	val companyDF = DBUtil.loadDFFromTable("dc_import."+province+"_company", 16, spark).select("id", "type", "name", "md5")
        appendCompanyByProvince(companyDF)
      }
    }
    
  }
  
  def appendCompanyByProvince(companyDF:Dataset[Row], mode:SaveMode = SaveMode.Append){
//        companyDF.select("id", "name", "md5").write.mode(SaveMode.Append).json(GlobalVariables.companyBasicJsonDir)
//        println(s"*********************${province} all************************")
      //取出个体户，保留其余公司，用于计算企业投资等
      val df = companyDF.filter(row => {
        val typeName = row.getAs[String]("type")
        typeName == null || !typeName.startsWith("个体")
      }).select("id", "name", "md5")
      
//      df.write.mode(SaveMode.Append).json(GlobalVariables.companyBasicNoGetiJsonDir)
      HDFSUtil.saveDSToHDFS(GlobalVariables.hdfsCompanyDir, df, mode)
      companyDF.unpersist()
  }
}