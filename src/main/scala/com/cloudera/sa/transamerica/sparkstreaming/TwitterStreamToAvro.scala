package com.cloudera.sa.transamerica.sparkstreaming

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.google.gson.Gson

/**
 * Created by gmedasani on 8/25/16.
 */
object TwitterStreamToAvro {

  private var gson = new Gson()

  def main(args: Array[String]) {
    if (args.length < 5) {
      System.err.println("Usage: TwitterStreamToAvro <consumer key> <consumer secret> " +
        "<access token> <access token secret> <output-directory>")
      System.exit(1)
    }

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val outputDir = args(4)
//    val filters = args.takeRight(args.length - 4)

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)


    val sparkConf = new SparkConf().setAppName("TwitterStreamToAvro")
//      .setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(10))
    val tweetStream = TwitterUtils.createStream(ssc, None).map(tweet => gson.toJson(tweet))


    //Create a SparkSQL context
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    sqlContext.setConf("spark.sql.dialect","hiveql")

    tweetStream.foreachRDD(rdd => {

      //Create a dataframe from the RDD contain the JSON strings
      val rawTweetDF = sqlContext.read.json(rdd)

      //Extract the fields we are interested in
//      val tweetDF =  rawTweetDF.select("createdAt","user","text","retweetCount")

      //Updated version of the tweetDF
      val tweetDF = rawTweetDF.select("createdAt","user","text","retweetCount","id")

      //register tweetDF1 as a temptable
      tweetDF.registerTempTable("tweets_temp_table")

      //insert the tweets into Hive Table
      sqlContext.sql("INSERT into twitter_tweets_table SELECT * from tweets_temp_table ")

    })

    ssc.start()
    ssc.awaitTermination()
  }

}
