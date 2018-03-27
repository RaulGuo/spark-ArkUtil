package com.proud.ark.dizhi

import java.util.HashMap
import java.io.InputStreamReader
import java.io.FileInputStream
import java.io.BufferedReader
import com.google.gson.Gson

object ZhongguoDizhiUtil {
  
  import scala.collection.JavaConverters._
  
  val districtMap = getDistrictMap.asScala//县对应的省和市
	val districtSet = districtMap.keySet.filter { x => x.length() >= 2 }
	
	val	mapProvince = getMapProvince.asScala.+("香港" -> "香港", "澳门" -> "澳门", "台湾" -> null)
	val mapCity = getMapCity.asScala.+("襄樊" -> "湖北")
	
	val citySet = mapCity.keySet.-("")
	val provinces = mapProvince.keySet.-("");
  
  val countries = Set("联邦", "英属", "蒙古", "朝鲜", "韩国", "日本", "菲律宾", "越南", "老挝", "柬埔寨", "缅甸", "泰国", "马来西亚", "文莱", "新加坡", "印度尼西亚", "东帝汶", "尼泊尔", "不丹", "孟加拉国", "印度", "巴基斯坦", "斯里兰卡", "马尔代夫", "哈萨克斯坦", "吉尔吉斯斯坦", "塔吉克斯坦", "乌兹别克斯坦", "土库曼斯坦", "阿富汗", "伊拉克", "伊朗", "叙利亚", "约旦", "黎巴嫩", "以色列", "巴勒斯坦", "沙特阿拉伯", "巴林", "卡塔尔", "科威特", "阿联酋", "阿曼", "也门", "格鲁吉亚", "亚美尼亚", "阿塞拜疆", "土耳其", "塞浦路斯", "芬兰", "瑞典", "挪威", "冰岛", "丹麦", "爱沙尼亚", "拉脱维亚", "立陶宛", "白俄罗斯", "俄罗斯", "乌克兰", "摩尔多瓦", "波兰", "捷克", "斯洛伐克", "匈牙利", "德国", "奥地利", "瑞士", "列支敦士登", "英国", "爱尔兰", "荷兰", "比利时", "卢森堡", "法国", "摩纳哥", "罗马尼亚", "保加利亚", "塞尔维亚", "马其顿", "阿尔巴尼亚", "希腊", "斯洛文尼亚", "克罗地亚", "意大利", "梵蒂冈", "圣马力诺", "马耳他", "西班牙", "葡萄牙", "安道尔", "埃及", "利比亚", "苏丹", "突尼斯", "阿尔及利亚", "摩洛哥", "加拿大", "美国", "墨西哥", "哥伦比亚", "委内瑞拉", "圭亚那", "法属圭亚那", "苏里南", "厄瓜多尔", "秘鲁", "玻利维亚", "巴西", "智利", "阿根廷", "乌拉圭", "巴拉圭")
	
  def getProvinceAndCity(add:String):Array[String] = {
    val address = add.trim()
    if(address == null || address.trim().isEmpty())
      return null
    
    val replace = address.replace("中国", "").replace("自治区", "")
    
    for(country <- countries){//是否属于其他国家的
      if(replace.startsWith(country) && !country.trim().isEmpty())
          return Array("其它", null)
    }
    
    val substringLength = if(replace.contains("市")) replace.indexOf("市")+1 else if(replace.length() < 8) replace.length() else 8
    val prefix = replace.substring(0, substringLength)
    
    //是否以某个省份开头
    var currProvince:String = null;
    for(province <- provinces)
      if(prefix.startsWith(province))
        currProvince = province
        
    for(city <- citySet){
      if(prefix.indexOf(city) >= 0 && prefix.indexOf(city+"路") < 0 ){
        if(currProvince != null && mapCity(city).equals(currProvince))//如果确定了省份，就可以用省份来过滤一下城市
          if(city != "吉林")
            return Array(mapCity(city), city)
          else{//吉林省吉林市
            if(prefix.contains("吉林市")){
              return Array(mapCity(city), city)
            }
          }
      }else if(city.length() > 4 && address.indexOf(city) > 0){
        return Array(mapCity(city), city)
      }
    }
    
    for(district <- districtSet)
      if(district.length() <= 4){
      	if(prefix.indexOf(district)>=0)
      	  if(currProvince != null && districtMap(district)(0).equals(currProvince))//如果确定了省份，就可以用省份来过滤一下所属的地区
        		return districtMap(district)
      }else{
        if(address.indexOf(district)>=0)
      	  if(currProvince != null && districtMap(district)(0).equals(currProvince))//如果确定了省份，就可以用省份来过滤一下所属的地区
        		return districtMap(district)
      }
    
    for(province <- provinces)
      if(prefix.indexOf(province)>=0)
        return Array(province, mapProvince(province))
    
        
    
    for(city <- citySet){
      if(address.indexOf(city) >= 0 && address.indexOf(city+"路") < 0 && address.indexOf(city) < 10){
        return Array(mapCity(city), city)
      }
    }
    
    for(district <- districtSet)
      if(address.indexOf(district)>=0)
        return districtMap(district)
    
    for(province <- provinces)
      if(address.indexOf(province)>=0)
        return Array(province, mapProvince(province))
    
    return Array("其它", null)
  }
  
  
  
  
  def getProvinceAndCityByCompanyName(add:String):Array[String] = {
    val address = add.trim()
    if(address == null || address.trim().isEmpty())
      return null
    
    val replace = address.replace("中国", "").replace("自治区", "")
    
    for(country <- countries){//是否属于其他国家的
      if(replace.startsWith(country) && !country.trim().isEmpty())
          return Array("其它", null)
    }
    
    val substringLength = if(replace.contains("市")) replace.indexOf("市")+1 else if(replace.length() < 8) replace.length() else 8
    val prefix = replace.substring(0, substringLength)
    
    //是否以某个省份开头
    var currProvince:String = null;
    for(province <- provinces)
      if(prefix.startsWith(province))
        currProvince = province
        
    for(city <- citySet){
      if(prefix.indexOf(city) >= 0){
        if(currProvince != null && mapCity(city).equals(currProvince))//如果确定了省份，就可以用省份来过滤一下城市
          if(city != "吉林")
            return Array(mapCity(city), city)
          else{//吉林省吉林市
            if(prefix.contains("吉林市")){
              return Array(mapCity(city), city)
            }
          }
      }else if(city.length() > 4 && address.indexOf(city) > 0){
        return Array(mapCity(city), city)
      }
    }
    
    for(district <- districtSet)
      if(district.length() <= 4){
      	if(prefix.indexOf(district)>=0)
      	  if(currProvince != null && districtMap(district)(0).equals(currProvince))//如果确定了省份，就可以用省份来过滤一下所属的地区
        		return districtMap(district)
      }else{
        if(address.indexOf(district)>=0)
      	  if(currProvince != null && districtMap(district)(0).equals(currProvince))//如果确定了省份，就可以用省份来过滤一下所属的地区
        		return districtMap(district)
      }
    
    for(province <- provinces)
      if(prefix.indexOf(province)>=0)
        return Array(province, mapProvince(province))
    
        
    
    for(city <- citySet){
      if(address.indexOf(city) >= 0 && address.indexOf(city) < 10){
        return Array(mapCity(city), city)
      }
    }
    
    for(district <- districtSet)
      if(address.indexOf(district)>=0)
        return districtMap(district)
    
    for(province <- provinces)
      if(address.indexOf(province)>=0)
        return Array(province, mapProvince(province))
    
    return Array("其它", null)
  }
  
  
  case class PCD(Province:String, City:String, District:String, Code:String)
  
  def getDistrictMap():HashMap[String, Array[String]] = {
    val reader = new InputStreamReader(new FileInputStream("/home/data_center/dependency/pro.txt"));
//    val reader = new InputStreamReader(new FileInputStream("D:\\workspace\\index-cron\\conf\\pro.txt"));
    val br = new BufferedReader(reader)
    val line = br.readLine();
    val gson = new Gson()
    val pcds = gson.fromJson(line, classOf[Array[PCD]])
    val districtMap = new HashMap[String, Array[String]]();
    
    pcds.foreach { x => {if(x.District != null && !x.District.trim().isEmpty()) districtMap.put(x.District.replace("市", "").replace("区", ""), Array(x.Province, x.City.replace("市", "")))} }
    
    districtMap
  }
  
  def getMapCity():HashMap[String, String] = {
    val reader = new InputStreamReader(new FileInputStream("/home/data_center/dependency/pro.txt"));
//    val reader = new InputStreamReader(new FileInputStream("D:\\workspace\\index-cron\\conf\\pro.txt"));
    val br = new BufferedReader(reader)
    val line = br.readLine();
    val gson = new Gson()
    val pcds = gson.fromJson(line, classOf[Array[PCD]])
    val mapCity = new HashMap[String, String]();
    
    pcds.foreach { x => {mapCity.put(x.City.replace("市", ""), x.Province)} }
    
    mapCity
  }
  
  def getMapProvince():java.util.HashMap[String, String] = {
    val mapProvince = new HashMap[String,String]();
		mapProvince.put("北京", "北京");
		mapProvince.put("上海", "上海");
		mapProvince.put("天津", "天津");
		mapProvince.put("重庆", "重庆");
		mapProvince.put("辽宁", "沈阳");
		mapProvince.put("吉林", "长春");
		mapProvince.put("黑龙江", "哈尔滨");
		mapProvince.put("内蒙古", "呼和浩特");
		mapProvince.put("山西", "太原");
		mapProvince.put("河北", "石家庄");
		mapProvince.put("山东", "济南");
		mapProvince.put("江苏", "南京");
		mapProvince.put("安徽", "合肥");
		mapProvince.put("浙江", "杭州");
		mapProvince.put("福建", "福州");
		mapProvince.put("广东", "广州");
		mapProvince.put("广西", "南宁");
		mapProvince.put("海南", "海口");
		mapProvince.put("湖北", "武汉");
		mapProvince.put("湖南", "长沙");
		mapProvince.put("河南", "郑州");
		mapProvince.put("江西", "南昌");
		mapProvince.put("宁夏", "银川");
		mapProvince.put("新疆", "乌鲁木齐");
		mapProvince.put("青海", "西宁");
		mapProvince.put("陕西", "西安");
		mapProvince.put("甘肃", "兰州");
		mapProvince.put("四川", "成都");
		mapProvince.put("云南", "昆明");
		mapProvince.put("贵州", "贵阳");
		mapProvince.put("西藏", "拉萨");
		mapProvince.put("台湾", "台北")
		
		mapProvince
  }
  
  def main(args: Array[String]): Unit = {
    val dizhi = "黄浦江西路"
    val result = getProvinceAndCity(dizhi)
    result.foreach(println)
  }
}