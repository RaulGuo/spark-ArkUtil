package com.proud.ark.data

object Util {
  def toDBC(str:String):String = {
    if(str == null)
      return null;
    val c = str.toCharArray();
    var cr = new Array[Char](c.length)
    var i = 0;
    c.foreach { x => {
      if(x  == '\u3000') {
        cr(i) = ' ';
      }else if(x > '\uFF00' && x < '\uFF5F'){
        cr(i) = (x - 65248).asInstanceOf;
      }
      i = i+1
    } }
    
    val result = new String(cr).trim()
    result;
  }
  
  def main(args: Array[String]): Unit = {
    val str = "江阴市供销合作总社　";
    println("------"+toDBC(str)+"-----")
  }
}