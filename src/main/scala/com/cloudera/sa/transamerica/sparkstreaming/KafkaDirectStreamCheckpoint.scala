package com.cloudera.sa.transamerica.sparkstreaming

import com.cloudera.sa.transamerica.util.KafkaOffsetDAO
import kafka.serializer.StringDecoder
import org.apache.log4j.{LogManager, Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Time, Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by gmedasani on 8/24/16.
 */
object KafkaDirectStreamCheckpoint {
  def main (args: Array[String]) {
    if (args.length < 5) {
      System.err.println("Usage: KafkaDirectStreamCheckpoint <batch-duration-in-seconds> <broker-list> <kafka-topics> <hbase-zookeeper-quorum> <checkpoint-dir>")
      System.exit(1)
    }

    val batchDuration = args(0)
    val brokers = args(1).toString()
    val topicsSet = args(2).toString.split(",").toSet
    val hbaseZkQuorum = args(3)
    val checkPointDir = args(4)


    def createNewStreamingContext():StreamingContext = {
      val sparkConf = new SparkConf().setAppName("Streaming Divvy Bikes - KafkaDirectStream ")
//        .setMaster("local[2]")
      val sc = new SparkContext(sparkConf)
      val ssc = new StreamingContext(sc, Seconds(batchDuration.toLong))
      ssc.checkpoint(checkPointDir)
      ssc
    }

    //Get StreamingContext from checkpoint data or create a new one
    val ssc = StreamingContext.getOrCreate(checkPointDir,() => createNewStreamingContext())

    //Setup the Kafka Parameters
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    //Create the Kafka DirectStream
    val divvyStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)


    //Convenience function to print the offsets
    def printOffsets(rdd: RDD[(String, String)], time: Time) = {
      val offsets = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      for (offset <- offsets){
        println(offset)
      }
    }

    //Print the offsets for each batch
    divvyStream.foreachRDD((rdd, time) => printOffsets(rdd, time))

    //Continue the main operation of the streaming application
//    divvyStream.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
