package com.proud.ark.ml

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.IDFModel
import org.apache.spark.mllib.classification.NaiveBayesModel
import com.proud.ark.db.ProdIntranetDBUtil
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.feature.IDF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import com.proud.ark.config.ConfigUtil
import com.proud.ark.data.HDFSUtil

/**
 * spark-submit --driver-memory 10g --class com.proud.ark.ml.TrainCompanyIndustryModel --jars /home/data_center/dependency/mysql-connector-java.jar,/home/data_center/dependency/ikanalyzer-2012_u6.jar /home/data_center/dependency/ArkUtil-0.0.1-SNAPSHOT.jar
 * spark-shell --jars /home/data_center/dependency/mysql-connector-java.jar,/home/data_center/dependency/ikanalyzer-2012_u6.jar,/home/data_center/dependency/ArkUtil-0.0.1-SNAPSHOT.jar
 */

object TrainCompanyIndustryModel {
	val seed = "生产、制造、新能源、乘用车、农用车、客车制造、纯电动整车、整车、电池、锂电池、镍氢电池、充电器、干电池、锂离子电池、锂离子动力电池、锂离子储能电池、新型电池、电芯、电池材料、动力电池、电池生产、锂离子电池、蓄电池、铅酸蓄电池、电机、电动机、微型电机、整车控制器、工程机械集成电控、控制软件、电机控制器系统、电池管理系统、高压电气盒、电池充放电器、、智能控制研发、整车控制器、驱动器，整车控制系统、、倒车辅助系统、入侵侦测系统、车载泊车摄像头、驾驶员信息系统，车辆内部模块、仪表仪器制造、速度表、油量计、组合仪表、发动机、铸造产品、柴油机组制造、发电机组制造、发电机、电动机制造、发动机总汇铸造加工、驱动系统零部件、气缸套、活塞、活塞环、活塞销、组合件、铸锻件加工、铸铁件、铸造模具、油泵轴、水泥建材机械、铸钢件、铸铁件、汽配件、空气悬架系统、排气管焊接、涡轮增压器、机油滤清器、柴油滤清器、空气滤清器、汽油滤清器、滤清器制造、油管、油箱、石油机具、油箱、油泵油嘴、内燃机、汽油、油管、油箱、石油机具、油箱、油泵油嘴、内燃机、柴油、散热器、车用暧风、进气管，排气管、刹车衬片、紧固件、离合器、制动器、制动器衬片、齿轮、变速箱、变速、换挡、轴承、传动轴、齿轮、传动装置、曲轴、半轴、车桥、整体桥、车轴、减振器、空气悬架、悬架、传动、制动鼓、制动盘、摩擦材料、制动器总成、轮毂、铝合金车轮、午线轮胎、航空轮胎、轮胎生产、电子控制制动防抱死系统，制动器系统、盘式制动器总成、鼓式制动器、音响设备、电子产品；、车用DVD、塑料产品、内饰件、车用地垫、方向盘套、香品、倒车镜、镜杆、交通用灯具、灯具、内外装饰件、天窗、全景、大梁、保险杠、座椅、内饰件、车座、坐垫、雨刷、雨刮器、雨刮、后视镜、电动后视镜、车镜";
  
  //对未执行分词的种子数据进行训练，获取训练模型的模板。
  //inputCol代表的分类的依据。
  //fenleiCol代表分类的结果。结果字段应该是固定的整数字段
  def getTrainModel(spark:SparkSession, df:Dataset[Row], inputCol:String, fenleiCol:String):(IDFModel, NaiveBayesModel) = {
    import spark.implicits._
    //包含内容：id, scope, fenlei_num
//    val df = ProdIntranetDBUtil.loadDFFromTable("", spark)
    
    val tokenColumn = "tokens"
    
    //分词
    val tokenFunction:Function1[String, Array[String]] = TokenCNUtil.token
    
    import org.apache.spark.sql.functions._
    val tokenUdf = udf(tokenFunction)
    
    val tokenDF = df.withColumn(tokenColumn, tokenUdf(col(inputCol)))
//    val tokenizer = new Tokenizer().setInputCol(inputCol).setOutputCol(tokenColumn)
//    val tokenDF = tokenizer.transform(df)
    
    //计算tf idf
    val termFreqCol = "term_freq"
    
    val tf = new HashingTF().setInputCol(tokenColumn).setOutputCol(termFreqCol).setNumFeatures(2048*10)
    val tfDF = tf.transform(tokenDF);
    val idf = new IDF().setInputCol(termFreqCol).setOutputCol("tf_idf")
    val idfModel = idf.fit(tfDF)
    val idfDF = idfModel.transform(tfDF);
    
    //NaiveBayes在train的时候对数据格式有要求，需要是RDD[LabeledPoint]类型的对象。
    //LabeledPoint对象中包括label和features两个字段，label是分类的结果，用double表示。feature是IDFModel计算得出的feature的tf-idf
    val metadata = idfDF.map(x => {
      val tfidf = x.getAs[org.apache.spark.ml.linalg.SparseVector]("tf_idf").toArray
      LabeledPoint(x.getAs[Int](fenleiCol), Vectors.dense(tfidf))
    }).rdd
    
    val nbModel:NaiveBayesModel = NaiveBayes.train(metadata)
    (idfModel, nbModel)
  }
  
  case class FenleiResult(id:Integer, scope:String, tokens:String, fenlei:Double)
  case class CarInfo(id:Integer, scope:String)
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("CompanyIndustryClassifier").config("spark.sql.warehouse.dir", ConfigUtil.warehouse).master(ConfigUtil.master).getOrCreate()
    import spark.implicits._
    val seedDF = ProdIntranetDBUtil.loadDFFromTable("chain.ml_types", spark).select("id", "keyword")
    val (idfModel, nbModel) = getTrainModel(spark, seedDF, "keyword", "id")
    
    //id, scope
//    val df = HDFSUtil.getDSFromHDFS("/home/data_center/CompanyScope", spark).select("id", "scope")
    /**
     * 对scope进行转换，将其中的特殊符号（包括逗号，句号，分号，冒号）全都替换成顿号。
     * 然后对scope按照顿号进行切割，切割后保留包含“车”关键字的短语。使用这个短语进行分词
     */
    
    val df = ProdIntranetDBUtil.loadDFFromTable("test.car_company_classify", 16, spark).select("id", "scope").map(x =>{
      val id = x.getAs[Integer]("id")
      val originScope = x.getAs[String]("scope").replace("：", "、").replace(":", "、").replace(",", "、").replace("，", "、").replace("。", "、").replace(".", "、").replace(";", "、").replace("；", "、").split("、")
      val filterScope = originScope.filter { x => x.contains("车") }.mkString(",")
      CarInfo(id, filterScope)
    })
    
    
    val tokenFunction:Function2[String, String, Array[String]] = TokenCNUtil.tokenRemainByString
    
    import org.apache.spark.sql.functions._
    val tokenUdf = udf(tokenFunction)
    //id, scope, tokens, tokens是分词的字段

//    val seedSet = TokenCNUtil.tokenToSet(seed)
    val tokenDF = df.withColumn("tokens", tokenUdf(col("scope"), lit(seed)))
    
    val tf = new HashingTF().setInputCol("tokens").setOutputCol("term_freq").setNumFeatures(2048*10)
    //id, scope, tokens, term_freq, term_freq是计算出的词频
    val tfDF = tf.transform(tokenDF);
    
    //idfModel的输入字段是term_freq, 输出字段是tf_idf。该字段是SparseVector。可以用训练的模型来进行预测。
    val idfDF = idfModel.transform(tfDF);
    
    
    val predictFunc:Function1[org.apache.spark.ml.linalg.SparseVector, Double] = (vector:org.apache.spark.ml.linalg.SparseVector) => {
    	val array = vector.toArray
			val v = Vectors.dense(array)
			nbModel.predict(v)
    }
    val predictUdf = udf(predictFunc)
    
    val result = idfDF.withColumn("fenlei", predictUdf(col("tf_idf"))).select("id", "scope", "tokens", "fenlei").map(x => {
      val id = x.getAs[Integer]("id")
      val scope = x.getAs[String]("scope")
      val tokens = x.getAs[scala.collection.mutable.WrappedArray[String]]("tokens").mkString(",")
      val fenlei = x.getAs[Double]("fenlei")
      FenleiResult(id, scope, tokens, fenlei)
    })
    
    
    
    ProdIntranetDBUtil.truncate("test.company_classify")
    ProdIntranetDBUtil.saveDFToDB(result, "test.company_classify")
    
  }
}