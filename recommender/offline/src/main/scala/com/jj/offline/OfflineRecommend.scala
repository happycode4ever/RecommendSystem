package com.jj.offline

import java.time.LocalDateTime

import com.jj.common.GlobalConf.globalConf
import com.jj.common.{Movie, OfflineUserRecommendation, Rating, Recommendation}
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.sql.{SaveMode, SparkSession}

object OfflineRecommend {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("OfflineRecommend").set("spark.executor.memory","6G").set("spark.driver.memory","2G")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
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
    spark.sqlContext.clearCache()
    spark.close()
  }
}
