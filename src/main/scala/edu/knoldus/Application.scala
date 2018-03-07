package edu.knoldus

import edu.knoldus.service.Twitter
import org.apache.log4j.{Level, Logger}

object Application {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    val twitter = new Twitter
    twitter.saveTopTweets
  }
}
