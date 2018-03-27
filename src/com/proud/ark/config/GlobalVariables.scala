package com.proud.ark.config

object GlobalVariables {
  //所有省份数据
  val provinces = Array("anhui","beijing","fujian","gansu","guangxi","hainan","hebei","heilongjiang","henan","hubei","hunan","jiangsu","jilin","liaoning","ningxia","qinghai","shandong","shanghai", "shanxi","tianjin","xinjiang","xizang","yunnan","zongju", "guangdong", "chongqing", "zhejiang", "sichuan", "guizhou", "neimenggu", "xianxi", "jiangxi");
//  val provinces = Array("liaoning","ningxia","qinghai","shandong","shanxi","tianjin","xinjiang","xizang","yunnan","zongju");
  
  val provinceCodeMap = Map("anhui" -> 1, "beijing" -> 2, "fujian" -> 3, 
    "gansu" -> 4, "guangxi" -> 5, "hainan" -> 6, "hebei" -> 7, "heilongjiang" -> 8, "henan" -> 9, "hubei" -> 10, "hunan" -> 11, "jiangsu" -> 12,
    "jilin" -> 13, "liaoning" -> 14, "ningxia" -> 15, "qinghai" -> 16, "shandong" -> 17, "shanghai" -> 18,
    "shanxi" -> 19, "tianjin" -> 20, "xinjiang" -> 21, "xizang" -> 22, "yunnan" -> 23, "zongju" -> 24,
    "guangdong" -> 25, "chongqing" -> 26, "zhejiang" -> 27, "sichuan" -> 28, "guizhou" -> 29, "neimenggu" -> 30, "xianxi" -> 31, "jiangxi" -> 32)
  
  val codeProvinceMap = provinceCodeMap.map(_.swap)
  
  val provNameCodeMap = Map("安徽" -> 1, "北京" -> 2, "福建" -> 3, 
    "甘肃" -> 4, "广西" -> 5, "海南" -> 6, "河北" -> 7, "黑龙江" -> 8, "河南" -> 9, "湖北" -> 10, "湖南" -> 11, "江苏" -> 12,
    "吉林" -> 13, "辽宁" -> 14, "宁夏" -> 15, "青海" -> 16, "山东" -> 17, "上海" -> 18,
    "山西" -> 19, "天津" -> 20, "新疆" -> 21, "西藏" -> 22, "云南" -> 23, "总局" -> 24,
    "广东" -> 25, "重庆" -> 26, "浙江" -> 27, "四川" -> 28, "贵州" -> 29, "内蒙古" -> 30, "陕西" -> 31, "江西" -> 32)
  
  val codeProvNameMap = provNameCodeMap.map(_.swap)
  
  //hdfs的企业基本信息（id,name,md5）的基本目录
  val hdfsCompanyDir = "/home/data_center/companyNoGeti"
  
  //整理的股东信息保存在HDFS中的路径。
  val hdfsGudongXinxi = "/home/data_center/GudongXinxi"
  
  //企业基本信息（id,name,md5）的保存在本地的基本目录
  val companyBasicJsonDir = "/home/data_center/company_basic"
  
  //取出了个体户的企业基本信息（id,name,md5）的保存在本地的基本目录
  val companyBasicNoGetiJsonDir = "/home/data_center/company_basic_no_geti"
  
  
}