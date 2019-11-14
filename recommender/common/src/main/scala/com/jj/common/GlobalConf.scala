package com.jj.common

/**
  * 全局配置集合包括mongodb配置和es配置
  */
object GlobalConf{
  //mongodb和es相关配置
  val globalConf = Map(
    //mongodb相关配置
    "mongodb.uri" -> "mongodb://192.168.1.127:27017",
    "mongodb.db" -> "recom",
    //源数据表位置
    "mongodb.collection.movies" -> "movies",
    "mongodb.collection.ratings" -> "ratings",
    "mongodb.collection.tags" -> "tags",
    //离线统计表位置
    "mongodb.collection.RateMoreMovies" -> "RateMoreMovies",
    "mongodb.collection.RateMoreMoviesRecently" -> "RateMoreMoviesRecently",
    "mongodb.collection.AverageMoviesScore" -> "AverageMoviesScore",
    "mongodb.collection.GenresTopMovies" -> "GenresTopMovies",
    //离线推荐表位置
    "mongodb.collection.UserRecs" -> "UserRecs",
    "mongodb.collection.MovieRecs" -> "MovieRecs",
    //实时推荐配置
    "kafka.topic" -> "recommend",
    "redis.host" -> "192.168.1.127",
    "redis.recently.rating.prefix" -> "uid:",
    "mongodb.collection.StreamRecs" -> "StreamRecs",
    //es相关配置
    "es.cluster.name" -> "es-cluster",
    "es.nodes" -> "192.168.1.127",
    "es.http.port" -> "9200",
    "es.tcp.port" -> "9300",
    "es.index" -> "recom",
    "es.type.movies" -> "movies"
  )

}
