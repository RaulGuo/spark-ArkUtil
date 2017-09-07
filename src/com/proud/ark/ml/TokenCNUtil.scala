package com.proud.ark.ml

import java.io.ByteArrayInputStream
import java.io.InputStreamReader
import org.wltea.analyzer.core.IKSegmenter
import org.wltea.analyzer.core.Lexeme

object TokenCNUtil {
  def token(content:String):Array[String] = {
    var set = tokenToSet(content)
    set.toArray
  }
  
  def tokenRemain(content:String, seedSet:Set[String]):Array[String] = {
    var set = Set[String]()
    val bt = content.getBytes;
    val inputStream = new ByteArrayInputStream(bt)
    val reader = new InputStreamReader(inputStream)
    val iksegmenter = new IKSegmenter(reader, false)
    
    var lexeme:Lexeme = null;
    while({lexeme = iksegmenter.next(); lexeme != null}){
      val tmp = lexeme.getLexemeText()
      if(seedSet.contains(tmp))
        set = set+lexeme.getLexemeText
    }
    
    val it = Iterator.continually(iksegmenter.next()).takeWhile { _ != null }
    set.toArray
  }
  
  def tokenRemainByString(content:String, seed:String):Array[String] = {
    val seedSet = tokenToSet(seed)
    tokenRemain(content, seedSet)
  }
  
  def tokenToSet(content:String):Set[String] = {
    var set = Set[String]()
    val bt = content.getBytes;
    val inputStream = new ByteArrayInputStream(bt)
    val reader = new InputStreamReader(inputStream)
    //参数中的boolean参数代表是否使用智能分词。为true代表使用智能分词，false代表最细粒度分词
    val iksegmenter = new IKSegmenter(reader, false)
    
    var lexeme:Lexeme = null;
    while({lexeme = iksegmenter.next(); lexeme != null}){
      set = set+lexeme.getLexemeText
    }
    
    val it = Iterator.continually(iksegmenter.next()).takeWhile { _ != null }
    set
  }
  
  def main(args: Array[String]): Unit = {
    val content = "销售工程师"
    println(token(content))
  }
}