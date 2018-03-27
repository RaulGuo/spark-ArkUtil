package com.proud.ark.ml

/**
 * 构建NaiveBayesClassifier中需要的模型
 */

import org.apache.spark.sql.SparkSession
import com.proud.ark.config.ConfigUtil
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.feature.IDF
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.classification.NaiveBayesModel
import com.proud.ark.data.HDFSUtil
import org.apache.spark.sql.SaveMode
import org.apache.spark.ml.feature.IDFModel
import org.apache.spark.sql.Row
import com.proud.ark.db.Prod153IntraDBUtil

object NBModelBuilder {
  
  def getHashingTF():HashingTF = {
    val tf = new HashingTF().setInputCol("tokens").setOutputCol("term_freq").setNumFeatures(2048*10)
    tf
  }
  
  def initNaiveBayesModel(spark:SparkSession):(IDFModel, NaiveBayesModel) = {
    import spark.implicits._
    val df = Prod153IntraDBUtil.loadDFFromTable("test.zhiwei_fenlei_training", spark)
    
    //1. 进行分词，输出字段tokens中是title_parsed的分词结果，是一个字符串的数组
    val tokenizer = new Tokenizer().setInputCol("title_parsed").setOutputCol("tokens")
    val tokenDF = tokenizer.transform(df)
    
    //2. 使用HashingTF计算Term Frequency。一个term在一个document中，出现了则tf为1，否则为0。
    val tf = getHashingTF()
    val tfDF = tf.transform(tokenDF)
    val idf = new IDF().setInputCol("term_freq").setOutputCol("idf")
    val idfModel = idf.fit(tfDF)
    val idfDF = idfModel.transform(tfDF)
    
    //NaiveBayes在train的时候对数据格式有要求，需要是RDD[LabeledPoint]类型的对象。
    //LabeledPoint对象中包括label和features两个字段，label是分类的结果，用double表示。feature是IDFModel计算得出的feature的tf-idf
    val metadata = idfDF.map(x => {
      val tfidf = x.getAs[org.apache.spark.ml.linalg.SparseVector]("idf").toArray
      LabeledPoint(x.getAs[Int]("fenlei_num"), Vectors.dense(tfidf))
    }).rdd
    
    val model:NaiveBayesModel = NaiveBayes.train(metadata)
    (idfModel, model)
  }
}