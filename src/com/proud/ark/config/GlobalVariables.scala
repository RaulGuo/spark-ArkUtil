package com.proud.ark.config

object GlobalVariables {
  //所有省份数据
  val provinces = Array("anhui","beijing","fujian","gansu","guangxi","hainan","hebei","heilongjiang","henan","hubei","hunan","jiangsu","jilin","liaoning","ningxia","qinghai","shandong","shanxi","tianjin","xinjiang","xizang","yunnan","zongju", "guangdong", "chongqing", "zhejiang", "sichuan", "guizhou", "neimenggu", "xianxi", "jiangxi");
//  val provinces = Array("liaoning","ningxia","qinghai","shandong","shanxi","tianjin","xinjiang","xizang","yunnan","zongju");
  
  //hdfs的企业基本信息（id,name,md5）的基本目录
  val hdfsCompanyDir = "/home/data_center/companyNoGeti"
  
  //整理的股东信息保存在HDFS中的路径。
  val hdfsGudongXinxi = "/home/data_center/GudongXinxi"
  
  //企业基本信息（id,name,md5）的保存在本地的基本目录
  val companyBasicJsonDir = "/home/data_center/company_basic"
  
  //取出了个体户的企业基本信息（id,name,md5）的保存在本地的基本目录
  val companyBasicNoGetiJsonDir = "/home/data_center/company_basic_no_geti"
  
  
}