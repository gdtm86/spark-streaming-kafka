package com.cloudera.sa.transamerica.sparkstreaming

import com.cloudera.sa.transamerica.util.{KafkaDirectDStreamUtil, KafkaOffsetDAO}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.Map

/**
 * Created by gmedasani on 8/24/16.
 */
object KafkaDirectStream {
  def main (args: Array[String]) {
    if(args.length < 6){
      System.err.println("Usage: KafkaDirectStreamSimple <batch-duration-in-seconds> <broker-list> <kafka-topics> <hbase-zookeeper-quorum> <habse-tableName> <offset-fetch-time>")
      System.exit(1)
    }

    val batchDuration = args(0)
    val brokers = args(1).toString
    val topicsSet= args(2).toString.split(",").toSet
    val zkQuorum = args(3)
    val tableName = args(4)
    val offsetFetchTime = args(5)


    //Create Spark configuration and create a streaming context
    val sparkConf = new SparkConf().setAppName("Streaming Divvy Bikes - KafkaDirectStream ")
//      .setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(batchDuration.toLong))
    val kafkaParams:scala.collection.mutable.Map[String,String] = Map[String,String]("metadata.broker.list" -> brokers)


    val topicADStream = KafkaDirectDStreamUtil.getKafkaDStream(offsetFetchTime:String, ssc:StreamingContext,
      kafkaParams,zkQuorum:String,tableName:String, topicsSet:Set[String])


    //define a function to save the offsets for each batch in HBase
    def saveOffsets(rdd:RDD[(String,String)],time: Time) = {
      val offsets = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      KafkaOffsetDAO.insertIntoHBase(offsets,zkQuorum,tableName,time)
    }

    topicADStream.foreachRDD((rdd,time) => saveOffsets(rdd,time))


    //Continue the main operation of the streaming application
    topicADStream.print()


    ssc.start()
    ssc.awaitTermination()

  }

}

