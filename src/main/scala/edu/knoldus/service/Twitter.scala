package edu.knoldus.service

import java.sql.DriverManager

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.Status

class Twitter {
  /**
    * store top 3 hashTag into MySql database.
    *
    * @return
    */
  def saveTopTweets: Boolean = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("twitter streaming")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Seconds(10))
    val stream: DStream[Status] = TwitterUtils.createStream(ssc, None)
    val hashTags: DStream[String] = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))
    val topHashTag: DStream[(Int, String)] = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(30))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))

    val url = "jdbc:mysql://localhost:3306/knoldus"
    val userName = "root"
    val password = "root"
    topHashTag.foreachRDD {
      rdd =>
        rdd.take(3).foreach {
          case (count, hashTag) =>
            val connection = DriverManager.getConnection(url, userName, password)
            val insertData = connection.prepareStatement("insert into Twitter(hashTag,count) values(?,?)")
            insertData.setString(1, hashTag)
            insertData.setInt(2, count)
            insertData.executeUpdate()
            connection.close()
            println("data inserted into database successfully")
        }
    }
    ssc.start()
    ssc.awaitTermination()
    true
  }
}