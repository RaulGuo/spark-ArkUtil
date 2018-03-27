package com.proud.ark.job

import java.io.File
import java.io.FileInputStream
import java.io.BufferedReader
import java.io.FileReader
import com.proud.ark.ml.TokenCNUtil
import java.util.HashMap
import java.util.Comparator
import java.util.ArrayList

object ShujuChanpinFenxi {
  
  case class Result(keyword:String, times:Integer);
  
  def main(args: Array[String]): Unit = {
    val file = new File("F:\\hello.txt");
    val br = new BufferedReader(new FileReader(file));
    var line:String = "";
    
    var map = Map[String, Integer]();
    var flag = true;
    while(flag){
      line = br.readLine();
      if(line != null){
        if(!line.trim.isEmpty()){
          val arr = TokenCNUtil.token(line)
          arr.foreach { x => {
            if(!map.contains(x)){
              map = map+((x -> 1))
            }else{
              map = map+((x, map(x)+1))
            }
          } }
        }
      }else{
        flag = false;
      }
    }
    
    val list = map.toList.sortBy(x => x._2)
    list.foreach(x => {println(x._1 +"=======;times:"+x._2)})    
    
  }
    
}