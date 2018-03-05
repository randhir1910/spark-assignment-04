package edu.knoldus

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.Status

object Application {
  val conf = new SparkConf().setMaster("local[*]").setAppName("Spark Streaming - PopularHashTags")
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Seconds(10))

    val stream: DStream[Status] = TwitterUtils.createStream(ssc, None)
    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))
    ssc.checkpoint("_checkpoint")
    val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))
      stream.print()
    topCounts60.foreachRDD(rdd => {
      val topList = rdd.take(3)
      topList.foreach { case (count, tag) => println(s"$tag ====   $count") }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
