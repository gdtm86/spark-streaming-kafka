package com.cloudera.sa.transamerica.util

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.collection.mutable.Map

/**
 * Created by gmedasani on 8/28/16.
 */
object KafkaDirectDStreamUtil {

  /*
*/
  def getKafkaDStream(offsetFetchTime:String,ssc:StreamingContext,kafkaParams:Map[String,String],zkQuorum:String,tableName:String,
                      topicsSet:Set[String]):InputDStream[(String,String)]= {
    if(offsetFetchTime == "beginning"){
      getKafkaDStreamFromBeginning(ssc:StreamingContext,kafkaParams,topicsSet:Set[String])
    } else {
      getKafkaDStreamFromOffsets(ssc,kafkaParams,zkQuorum,tableName,offsetFetchTime)
    }
  }


  /*
    */
  def getKafkaDStreamFromBeginning(ssc:StreamingContext,kafkaParams:Map[String,String], topicsSet:Set[String]
                                   ):InputDStream[(String, String)] = {

    kafkaParams += ("auto.offset.reset" -> "smallest")
    val inputDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,
                          kafkaParams.toMap,topicsSet)

    inputDStream
  }


  /*
  */
  def getKafkaDStreamFromOffsets(ssc:StreamingContext,kafkaParams:Map[String,String],zkQuorum:String,tableName:String,
                                 offsetFetchTime:String): InputDStream[(String,String)] ={

    val fromOffsets: scala.collection.immutable.Map[TopicAndPartition, Long] =
                            KafkaOffsetDAO.readOffsetsFromHBase(zkQuorum,tableName,offsetFetchTime).toMap

    def handleMessages(messageAndMetadata: MessageAndMetadata[String,String]):(String,String)= {
      (messageAndMetadata.key(),messageAndMetadata.message())
    }

    val inputDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder,(String,String)](
                            ssc,kafkaParams.toMap,fromOffsets,handleMessages _)

    inputDStream
  }


}
