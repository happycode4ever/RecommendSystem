package com.jj.offline

import java.time.LocalDateTime

import com.jj.common.GlobalConf.globalConf
import com.jj.common._
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.jblas.DoubleMatrix

object OfflineRecommend {
  /**
    * 计算用户推荐矩阵
    * 根据用户对电影的离线评分数据，构建训练的数据集。
    * 然后再根据全量的用户与电影数据集进行预测，得出每个用户的推荐电影项
    */
  def saveUserRecommendations(implicit spark:SparkSession): Unit ={
    import spark.implicits._
    //读取电影数据
    val movieDs = MongoSpark.load(spark, ReadConfig(Map(
      "uri" -> (globalConf("mongodb.uri") + "/" + globalConf("mongodb.db") + "." + globalConf("mongodb.collection.movies"))
    ))).as[Movie].cache()
    //读取评分数据(uid,mid,score)
    val ratingsDs = MongoSpark.load(spark, ReadConfig(Map(
      "uri" -> (globalConf("mongodb.uri") + "/" + globalConf("mongodb.db") + "." + globalConf("mongodb.collection.ratings"))
    ))).as[Rating].cache()
    val MovieRatingRDD = ratingsDs.rdd.map(moviesRating => recommendation.Rating(moviesRating.uid,moviesRating.mid,moviesRating.score))
    //用户id和电影id的笛卡尔积rdd 注意一个用户有多个评分，需要去重！！
    val uidRDD = ratingsDs.rdd.map(_.uid).distinct()
    val midRDD = movieDs.rdd.map(_.mid)
    val userProductRDD = uidRDD.cartesian(midRDD)
    //根据训练集构建模型
    val model = ALS.train(MovieRatingRDD,50,5,0.01)
    //根据模型，对用户与物品的全量数据集进行预测
    val preRatingRDD = model.predict(userProductRDD)
    //根据预测结果聚集用户id提取前10的电影信息最为推荐项
    val ourDf = preRatingRDD.filter(_.rating > 0).map(preRating => (preRating.user, (preRating.product, preRating.rating)))
      .groupByKey().map{case (uid, items) =>
      OfflineUserRecommendation(uid, items.toList.sortWith(_._2 > _._2).take(10).map(x => Recommendation(x._1,x._2)))
    }.toDF()
    MongoSpark.save(ourDf.write.mode(SaveMode.Overwrite).options(Map(
      "uri" -> (globalConf("mongodb.uri") + "/" + globalConf("mongodb.db") + "." + globalConf("mongodb.collection.UserRecs"))
    )))
  }

  /**
    * 计算电影相似度矩阵
    */
  def saveMovieRecommendations(implicit spark:SparkSession): Unit ={
    import spark.implicits._
    //1.根据评分数据训练集构建模型
    val ratingsDs = MongoSpark.load(spark, ReadConfig(Map(
      "uri" -> (globalConf("mongodb.uri") + "/" + globalConf("mongodb.db") + "." + globalConf("mongodb.collection.ratings"))
    ))).as[Rating]
    val trainRDD = ratingsDs.rdd.map(movieRatings => recommendation.Rating(movieRatings.uid,movieRatings.mid,movieRatings.score)).cache()
    val model = ALS.train(trainRDD,50,5,0.01)
    //2.从模型里提取电影特征矩阵，并将Array[Double]做转换为DoubleMatrix
    val productFeatures = model.productFeatures.map{case (mid,features) => (mid, new DoubleMatrix(features))}
    //3.根据每个电影的特征数组计算余弦相似度
    val simPFRDD = productFeatures.cartesian(productFeatures)//电影特征矩阵笛卡尔积
      //剔除相同mid
      .filter{case (pf1,pf2) => pf1._1 != pf2._1}
      .map{case (pf1,pf2) =>
        //计算两个矩阵的相似度包装成(mid1,(mid2,sim))这种结构方便聚合
        (pf1._1,(pf2._1,cosineSimilarity(pf1._2,pf2._2)))
      }
      //再筛选相似度大于0.6的电影mid
      .filter(_._2._2 > 0.6)
    //4.按照电影分组构建电影推荐数据
    val omrDf = simPFRDD.groupByKey().map{case (mid,items) => OfflineMovieRecommendation(mid,items.toList.map(x => Recommendation(x._1,x._2)))}.toDF()
    MongoSpark.save(omrDf.write.mode(SaveMode.Overwrite).options(Map(
      "uri" -> (globalConf("mongodb.uri") + "/" + globalConf("mongodb.db") + "." + globalConf("mongodb.collection.MovieRecs"))
    )))
  }

  /**
    * 计算两个矩阵的余弦相似度
    */
  def cosineSimilarity(dm1:DoubleMatrix,dm2:DoubleMatrix):Double = {
    dm1.dot(dm2)/(dm1.norm2() * dm2.norm2())
  }
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("OfflineRecommend").set("spark.executor.memory","6G").set("spark.driver.memory","2G")
    implicit val spark = SparkSession.builder().config(sparkConf).getOrCreate()

//    saveUserRecommendations
    saveMovieRecommendations

    spark.sqlContext.clearCache()
    spark.close()
  }
}
