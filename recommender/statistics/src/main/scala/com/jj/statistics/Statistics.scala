package com.jj.statistics

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId}

import com.jj.common.{GenresStatistics, Recommendation}
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.{SparkConf, sql}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import com.jj.common.GlobalConf._
import org.apache.spark.sql.types.DataTypes
import sql.functions._

object Statistics {

  //1.优质电影-电影评分数量统计
  def rateMoreMovies(implicit spark:SparkSession) = {
    val rmmDf = spark.sql("select mid,count(score) count from ratings group by mid order by count desc")//.show()
    MongoSpark.save(rmmDf.write.mode(SaveMode.Overwrite).options(Map(
      "uri" -> globalConf("mongodb.uri"),
      "database" -> globalConf("mongodb.db"),
      "collection" -> globalConf("mongodb.collection.RateMoreMovies")
    )))
  }
  //2.最近优质电影-按月为单位最近评分数最多的电影统计
  def rateMoreMoviesRecently(implicit spark:SparkSession): Unit ={
    //注册udf函数 将时间戳变更为yyyyMM的形式按月统计
    spark.udf.register("monthFormat",(time:Int) => {
      //时间戳存储少了3位，补充*1000L并且转换为long，不然丢失精度
      val ldt = Timestamp.from(Instant.ofEpochMilli(time * 1000L)).toLocalDateTime
      ldt.format(DateTimeFormatter.ofPattern("yyyyMM")).toLong
    })

    //先转换时间戳格式成临表，再从临表里按mid和month统计评分个数，最后按照月和个数降序排序
    val rmmrDf = spark.sql("select mid,month,count(score) count from " +
      "(select uid,mid,score,monthFormat(timestamp) month from ratings) t " +
      "group by month,mid order by month desc,count desc")//.show()

    MongoSpark.save(rmmrDf.write.mode(SaveMode.Overwrite).options(Map(
      "uri" -> globalConf("mongodb.uri"),
      "database" -> globalConf("mongodb.db"),
      "collection" -> globalConf("mongodb.collection.RateMoreMoviesRecently")
    )))
  }
  //最热电影-电影平均分统计
  def averageMoviesScore(implicit spark:SparkSession): Unit ={
    //按照mid聚合求平均分降序排序
    val amsDf= spark.sql("select mid,avg(score) avg from ratings group by mid order by avg desc")//.show()
    MongoSpark.save(amsDf.write.mode(SaveMode.Overwrite).options(Map(
      "uri" -> globalConf("mongodb.uri"),
      "database" -> globalConf("mongodb.db"),
      "collection" -> globalConf("mongodb.collection.AverageMoviesScore")
    )))
  }
  //每种类别最热电影-电影类别列转行平均分统计
  def genresTopMovies(implicit spark:SparkSession): Unit ={
    import spark.implicits._
    /**
      *     方式一、纯sql语句进行统计
      */
      /*

    //4.取编号前10的作为统计结果
    val frame = spark.sql("select mid,genre,avg,rank from " +
      //3.over开窗函数对同一类型下的电影排序生成编号
      "(select mid,genre,avg,row_number() over(partition by genre order by avg desc) rank from" +
      //2.join之前统计好的电影均分表
      "(select em.mid,em.genre,ams.avg from ams join " +
      //1.切割电影表的类型列形成数组，再炸开行转列操作形成临表
      "(select mid,genre from movies lateral view explode(split(genres,'\\\\|')) as genre) em " +
      "on ams.mid = em.mid) t) t2 " +
      "where rank <= 10 and order by genre,rank").cache()
    frame.show(100)
    frame
      //5.行转列操作，按照类别聚合，rank作为纵轴列，单元格包含mid和avg信息
      .groupBy("genre").pivot("rank").agg(sum("mid").cast(DataTypes.IntegerType).as("mid"),sum("avg").as("avg"))
      //        .printSchema()
      //6.df转rdd构建成需要的数据类型(genre,List((mid,avg),(mid,avg)...)) 这里暂且只构建前2名数据作为参考了
      .rdd.map(row => GenresStatistics(row.getString(0),List(Recommendation(row.getInt((1)),row.getDouble(2)),Recommendation(row.getInt((3)),row.getDouble(4)))))
          .collect().foreach(println)

          */
    //方式二，在join完后按照类别groupByKey，scala方式排序提取前10的电影均分信息
      //1.join操作
    val frame = spark.sql("select em.mid,em.genre,ams.avg from ams join" +
      "(select mid,genre from movies lateral view explode(split(genres,'\\\\|')) as genre) em " +
      "on ams.mid = em.mid")//.show()
    //2.变更数据为(genre,(mid,avg))
    val genresResultDf = frame.rdd.map(row => (row.getAs[String]("genre"), (row.getAs[Int]("mid"), row.getAs[Double]("avg"))))
      //3.按照genre聚合
      .groupByKey().map { case (genre, items) => {
      //4.scala进行组内平均值降序排序,取前10,并做数据转换
      GenresStatistics(genre, items.toList.sortWith(_._2 > _._2).take(10).map(item => Recommendation(item._1, item._2)))
    }
    }.toDF()
    MongoSpark.save(genresResultDf.write.mode(SaveMode.Overwrite).options(Map(
      "uri" -> globalConf("mongodb.uri"),
      "database" -> globalConf("mongodb.db"),
      "collection" -> globalConf("mongodb.collection.GenresTopMovies")
    )))
//      .takeSample(false,10,11L).foreach(println) //抽样验证没问题
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("Statistics")
    implicit val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    //读取mongodb://192.168.1.127:27017/recom.ratings需要的参数，三项不能少
    val ratingsReadConfig = ReadConfig(Map(
      "uri" -> globalConf("mongodb.uri"),
      "database" -> globalConf("mongodb.db"),
      "collection" -> globalConf("mongodb.collection.ratings")
    ))
    //读取加载好的用户对电影的评分表数据
    val ratingsDf = MongoSpark.load(spark,ratingsReadConfig).cache()
    ratingsDf.createOrReplaceTempView("ratings")
    //如果需要变更读取的collection直接通过withOption可以修改

    //    rateMoreMovies
    //    rateMoreMoviesRecently
    //    averageMoviesScore

    //**读取之前统计好的电影平均分表mid avg
    val averageReadConfig = ratingsReadConfig.withOption("collection",globalConf("mongodb.collection.AverageMoviesScore"))
    val amsDf = MongoSpark.load(spark,averageReadConfig).cache()
    amsDf.createOrReplaceTempView("ams")
    //读取电影信息表mid genres
    val moviesReadConfig = ratingsReadConfig.withOption("collection",globalConf("mongodb.collection.movies"))
    val moviesDf = MongoSpark.load(spark,moviesReadConfig).cache()
    moviesDf.createOrReplaceTempView("movies")

    genresTopMovies

    //最终清除缓存
    ratingsDf.unpersist()
    amsDf.unpersist()
    moviesDf.unpersist()
    //关闭spark
    spark.close()
  }
}
