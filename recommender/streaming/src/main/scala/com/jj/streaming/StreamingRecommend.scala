package com.jj.streaming

import com.jj.common.{OfflineMovieRecommendation, StreamKafkaRecord}
import com.jj.common.GlobalConf.globalConf
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks


object ConnectHelper extends Serializable {
  //lazy方式获取连接节省资源
  lazy val jedis = new Jedis(globalConf("redis.host"))
  lazy val mongoClient = MongoClient(MongoClientURI(globalConf("mongodb.uri")))
}

object StreamingRecommend {
  //提取用户最近电影评分的个数
  val USER_RECENTLY_RATING_COUNT = 20
  //提取最相似电影的个数
  val MOVIE_SIMILARITY_COUNT = 20
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  //  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

  /**
    * 从redis获取用户最近电影评分
    *
    * @param uid
    * @return
    */
  def getUserRecentlyMovieRatings(uid: Int) = {
    ConnectHelper.jedis
      .lrange(globalConf("redis.recently.rating.prefix") + uid, 0, USER_RECENTLY_RATING_COUNT)
      .map(line => {
        val attrs = line.split("\\|")
        (attrs(0).toInt, attrs(1).toDouble)
      }).toList
  }

  /**
    * 根据电影相似度矩阵，相似列表中，剔除用户已经已经评分过的电影，再按相似度排序提取设定的MOVIE_SIMILARITY_COUNT
    *
    * @param broadCastMovieRecs
    * @param uid
    * @param mid
    * @return
    */
  def getMovieMostSimilarity(broadCastMovieRecs: Map[Int, Map[Int, Double]], uid: Int, mid: Int): List[Int] = {
    //获取该用户已经评过分的电影
    val mongoClient = MongoClient(MongoClientURI(globalConf("mongodb.uri")))
    val ratingMids = mongoClient(globalConf("mongodb.db"))(globalConf("mongodb.collection.ratings"))
      .find(MongoDBObject("uid" -> uid))
      .toList.map(obj => obj.get("mid").toString.toInt)
    //从相似度矩阵中剔除已经评过分的电影
    val resultlist = broadCastMovieRecs.get(mid) match {
      case None => List() //问题点，相似度矩阵没有怎么办？
      case Some(recs) => {
        //从相似列表中，筛选出没有评过分的
        recs.toList.filter(rec => !ratingMids.contains(rec._1))
          //按照相似度排序取前20
          .sortWith(_._2 > _._2).map(_._1).take(MOVIE_SIMILARITY_COUNT)
      }
    }
    //最终返回list
    resultlist
  }

  /**
    * 核心算法：根据相似度矩阵，最近用户评分，最相似电影计算推荐电影的权重
    * @param broadCastMovieRecs 相似度矩阵
    * @param recentlyMovieRatings 最近用户评分
    * @param mostSimMovies 最相似电影
    * @return
    */
  def caculateRecommendMovies(broadCastMovieRecs: Map[Int, Map[Int, Double]], recentlyMovieRatings: List[(Int, Double)], mostSimMovies: List[Int]):List[(Int,Double)] = {
    //三个入参 相似矩阵，用户最近评分的M个电影例如ABC，N个相似的电影XYZ
    //算法功能，遍历N和M，从相似矩阵中找出每个N的相似电影(相似度要大于0.6)，去和M碰撞看看有没评过分，有则相乘取平均
    //然后评分如果超过3分，增强因子加1，没过3分，减弱因子加1
    //整体公式
    //sim(X,A)*R(A)+sim(X,B)*R(B)+sim(X,C)*R(C)/count(sim>0.6) + log2(增强因子和)-log2(减弱因子和)

    //用于存储sim(X,A)*R(A)，相似度和评分乘积
    val simRatingArr: ArrayBuffer[(Int, Double)] = ArrayBuffer()
    //增强因子
    val enhanceFactor: mutable.Map[Int, Int] = mutable.Map()
    //减弱因子
    val weakenFactor: mutable.Map[Int, Int] = mutable.Map()
    for (simMovie <- mostSimMovies; movieRating <- recentlyMovieRatings) {
      //如果该电影没有相似矩阵跳过
      val movieRecsOpt = broadCastMovieRecs.get(simMovie)
      if (movieRecsOpt.isDefined) {
        //从该电影的相似矩阵中，寻找有没该用户的评分记录
        val movieSim = movieRecsOpt.get.getOrElse(movieRating._1, 0.0)
        //如果有并且相似度高于0.6的则做乘积
        if (movieSim > 0.6) {
          //存储乘积
          simRatingArr += ((simMovie, movieSim * movieRating._2))
          //存储因子
          if (movieRating._2 > 3) {
            //从Map中提取该电影的记录+1，没有则赋值0+1
            enhanceFactor += (simMovie -> (enhanceFactor.getOrElse(simMovie, 0) + 1))
          } else {
            weakenFactor += (simMovie -> (enhanceFactor.getOrElse(simMovie, 0) + 1))
          }
        }
      }
    }
    //按照电影id分组计算乘积和以及因子
    simRatingArr.groupBy(_._1)
      .map { case (mid, simRatingArr:ArrayBuffer[(Int,Double)]) => {
        //提取乘积求和取均值，加上增强因子对数，减去减弱因子对数
        (mid,simRatingArr.map(_._2).sum / simRatingArr.length + log2X(enhanceFactor(mid)) - log2X(weakenFactor(mid)))
      }
      }.toList.sortWith(_._2 > _._2)
  }

  //换底公式lnX/ln2 = log2X
  def log2X(x: Double): Double = {
    math.log(x) / math.log(2)
  }
  //保存结果到mongodb
  def saveStreamRecsToMongoDB(uid:Int,streamRecsList:List[(Int,Double)]): Unit ={
    val mongoClient = MongoClient(MongoClientURI(globalConf("mongodb.uri")))
    val StreamRecsCollection = mongoClient(globalConf("mongodb.db"))(globalConf("mongodb.collection.StreamRecs"))
    //实时推荐覆盖原有数据，先删除再插入 (uid->uid,recs->"mid:weight|mid:weight|...")
    StreamRecsCollection.findAndRemove(MongoDBObject("uid" -> uid))
    StreamRecsCollection.insert(MongoDBObject("uid" -> uid,"recs" -> streamRecsList.map(rec => rec._1+":"+rec._2).mkString("|")))
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("StreamingRecommend")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(2))
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    val kafkaParam = Map(
      //配置broker地址
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "192.168.1.127:9092",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      //配置自动管理offset
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      //消费者组
      ConsumerConfig.GROUP_ID_CONFIG -> "recomGroup"
    )
    //从kafka提取用户刚刚评分的电影 uid|mid|score|timestamp
    //从Redis获取用户最近评分的M个电影 uid:uid lrange[mid|score]
    //从mongodb中提取电影相似度矩阵，过滤掉该用户已经评分过后的K个电影

    //从mongodb装载电影相似度矩阵转换为Map[mid:Int,Map[mid:Int,sim:Double]]，并且设为广播变量
    val movieRecs = MongoSpark.load(spark, ReadConfig(Map(
      "uri" -> (globalConf("mongodb.uri") + "/" + globalConf("mongodb.db") + "." + globalConf("mongodb.collection.MovieRecs"))
    )))
      .as[OfflineMovieRecommendation]
      .rdd.map {
      //转换RDD[OfflineMovieRecommendation] => RDD[(Int,Map(Int,Double))]
      case OfflineMovieRecommendation(mid, recs) => (mid, recs.map(rec => (rec.mid, rec.score)).toMap)
    } //最后collect成Map
      .collectAsMap()
    //将相似度矩阵设为广播变量
    val broadCastMovieRecs = sc.broadcast(movieRecs)


    //0.10版本的kafkaUtils和0.11版本的createDirectStream存在区别
    val kafkaDStream = KafkaUtils.createDirectStream[String, String](
      ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](List(globalConf("kafka.topic")), kafkaParam))

    kafkaDStream.map(record => {
      //      println(s"key:${record.key()},value:${record.value()}")
      //消息格式uid|mid|score|timestamp
      val attrs = record.value().split("\\|")
      StreamKafkaRecord(attrs(0).toInt, attrs(1).toInt, attrs(2).toDouble, attrs(3).toInt)
    })
      .foreachRDD(rdd => {
        rdd.foreach(msg => {
          val uid = msg.uid
          val mid = msg.mid
          //从redis提取该用户最近的电影评分，数据格式key->uid:uid value->lrange[mid|score]
          /*
          val recentlyMovieRatings = ConnectHelper.jedis
            .lrange(globalConf("redis.recently.rating.prefix")+uid, 0, USER_RECENTLY_RATING_COUNT)
            .map(line => {
              val attrs = line.split("\\|")
              (attrs(0).toInt, attrs(1).toDouble)
            })
        })//.count() //坑点：rdd前面只进行了map操作，需要action触发计算
        */
          //获取用户最近评分电影
          val recentlyMovieRatings = getUserRecentlyMovieRatings(uid)
          println(s"recentlyMovieRatings:$recentlyMovieRatings")
          //获取最相似的电影（已经剔除评分过的）
          val mostSimMovies = getMovieMostSimilarity(broadCastMovieRecs.value.toMap, uid, mid)
          println(s"mostSimMovies:$mostSimMovies")
          val streamRecsList = caculateRecommendMovies(broadCastMovieRecs.value.toMap,recentlyMovieRatings,mostSimMovies)
          println(s"streamRecsList:$streamRecsList")
          saveStreamRecsToMongoDB(uid,streamRecsList)
        })
      })

    //启动ssc
    ssc.start()
    ssc.awaitTermination()
  }
}

