package com.jj.dataloader

import java.net.InetAddress

import com.jj.common._
import com.jj.common.GlobalConf._
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import com.mongodb.spark.MongoSpark
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.spark.sql.EsSparkSQL
import org.elasticsearch.transport.client.PreBuiltTransportClient

object DataLoader {
  //读取源数据的路径配置
  val moviescsvPath = "H:\\bigdata-dev\\ideaworkspace\\tanzhou\\RecommendSystem\\recommender\\dataloader\\src\\main\\resources\\small\\movies.csv"
  val ratingscsvPath = "H:\\bigdata-dev\\ideaworkspace\\tanzhou\\RecommendSystem\\recommender\\dataloader\\src\\main\\resources\\small\\ratings.csv"
  val tagscsvPath = "H:\\bigdata-dev\\ideaworkspace\\tanzhou\\RecommendSystem\\recommender\\dataloader\\src\\main\\resources\\small\\tags.csv"

  def storeDataToMongoDB(movieDf: DataFrame, ratingDf: DataFrame, tagDf: DataFrame)(implicit mongoConfig: MongoConfig): Unit = {
    //建立客户端连接
    val client = MongoClient(MongoClientURI(mongoConfig.uri))
    //利用客户端删除对应的集合（表）
    client(mongoConfig.db)(globalConf("mongodb.collection.movies")).dropCollection()
    client(mongoConfig.db)(globalConf("mongodb.collection.ratings")).dropCollection()
    client(mongoConfig.db)(globalConf("mongodb.collection.tags")).dropCollection()
    //采用MongoSpark的API保存进mongodb，采用覆盖模式相当于把表删了
    MongoSpark.save(movieDf.write.mode(SaveMode.Overwrite).options(Map(
      "uri" -> globalConf("mongodb.uri"),
      "database" -> globalConf("mongodb.db"),
      "collection" -> globalConf("mongodb.collection.movies")
    )))
    MongoSpark.save(ratingDf.write.mode(SaveMode.Overwrite).options(Map(
      "uri" -> globalConf("mongodb.uri"),
      "database" -> globalConf("mongodb.db"),
      "collection" -> globalConf("mongodb.collection.ratings")
    )))
    MongoSpark.save(tagDf.write.mode(SaveMode.Overwrite).options(Map(
      "uri" -> globalConf("mongodb.uri"),
      "database" -> globalConf("mongodb.db"),
      "collection" -> globalConf("mongodb.collection.tags")
    )))
    //建完表后创建索引,1升序，-1降序，使用MongoDBObject构建
    client(mongoConfig.db)(globalConf("mongodb.collection.movies")).createIndex(MongoDBObject("mid"->1))
    client(mongoConfig.db)(globalConf("mongodb.collection.ratings")).createIndex(MongoDBObject("mid"->1))
    client(mongoConfig.db)(globalConf("mongodb.collection.ratings")).createIndex(MongoDBObject("uid"->1))
    client(mongoConfig.db)(globalConf("mongodb.collection.tags")).createIndex(MongoDBObject("mid"->1))
    client(mongoConfig.db)(globalConf("mongodb.collection.tags")).createIndex(MongoDBObject("uid"->1))
    //关闭连接
    client.close()
  }

  def storeDateToES(movieESDf: DataFrame) = {
    //构建ES客户端
    val client = new PreBuiltTransportClient(Settings.builder().put("cluster.name","es-cluster").build())
    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(globalConf("es.nodes")),globalConf("es.tcp.port").toInt))
    //判断索引是否存在，存在则删除
    val indicesAdminClient = client.admin().indices()
    if(indicesAdminClient.prepareExists(globalConf("es.index")).get().isExists){
      indicesAdminClient.prepareDelete(globalConf("es.index")).get()
    }
    //通过EsSparkSql保存索引信息,如果索引不存在会帮忙创建
    //def saveToEs(srdd: Dataset[_], resource: String, cfg: Map[String, String]): Unit
    //srdd表示要保存的数据，resource格式 index/type cfg表示配置，从官网拷贝，es.mapping.id表示用数据的指定列作为document的id
    EsSparkSQL.saveToEs(movieESDf,
      globalConf("es.index")+"/"+globalConf("es.type.movies"),
      Map("es.nodes" -> globalConf("es.nodes"),"es.port" -> globalConf("es.http.port"),"es.mapping.id" -> "mid"))

  }

  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("DataLoader")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext

    //用于转换DF
    import spark.implicits._
    //转换源数据为csv
    val movieDf = sc.textFile(moviescsvPath).map(line => {
      val attrs = line.split("\\^")
      Movie(attrs(0).trim.toInt, attrs(1).trim, attrs(2).trim, attrs(3).trim, attrs(4).trim, attrs(5).trim, attrs(6).trim, attrs(7).trim, attrs(8).trim, attrs(9).trim)
    }).toDF()
    val ratingDf = sc.textFile(ratingscsvPath).map(line => {
      val attrs = line.split(",")
      Rating(attrs(0).toInt,attrs(1).toInt,attrs(2).toDouble,attrs(3).toInt)
    }).toDF
    val tagDf = sc.textFile(tagscsvPath).map(line => {
      val attrs = line.split(",")
      Tag(attrs(0).toInt,attrs(1).toInt,attrs(2),attrs(3).toInt)
    }).toDF

    //保存三组DF到MongoDB
    implicit val mongoConfig = MongoConfig(globalConf("mongodb.uri"),globalConf("mongodb.db"))
//    storeDataToMongoDB(movieDf,ratingDf,tagDf)

    //保存movie和tag表join之后的数据到ES
    import sql.functions._
    //对于标签表进行行转列操作，用|聚合用于对于电影的标签
    val transTagDf = tagDf.groupBy("mid").agg(concat_ws("|",collect_set("tag")).as("tags"))//.show()
    //join操作 连接表达式可以使用Seq(c1,c2) c1是左表字段，c2是右表字段，或者直接使用df1(c1)===df2(c2)
    //这里存在电影可能没标签也需要导入ES，所以使用右外连接
//    transTagDf.join(movieDf,Seq("mid","mid"),"right").where(transTagDf("mid").isNotNull).show(false)
    val movieESDf = transTagDf.join(movieDf,Seq("mid","mid"),"right")
      .select("mid","name","descri","timelong","issue","shoot","language","genres","actors","directors","tags")//.show(false)
    storeDateToES(movieESDf)
    spark.close()
  }
}
