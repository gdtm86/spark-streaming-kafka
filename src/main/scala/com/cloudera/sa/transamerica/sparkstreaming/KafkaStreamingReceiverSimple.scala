package com.cloudera.sa.transamerica.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils

/**
 * Created by gmedasani on 8/23/16.
 */
object KafkaStreamingReceiverSimple {

  def main (args: Array[String]) {
    if(args.length < 4){
      System.err.println("Usage: KafkaStreamingSimple <batch-duration-in-seconds> <zookeeper-quorum> <kafka-topic> <kafka-consumer-group>")
      System.exit(1)
    }

    val batchDuration = args(0)
    val zkQuorum = args(1)
    val kakfaTopics = List((args(2),1)).toMap
    val consumerGroup = args(3)

    //Create Spark configuration and create a streaming context
    val sparkConf = new SparkConf().setAppName("Streaming Divvy Bikes").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(batchDuration.toLong))

    //Create the Kafka stream
    val logsStream = KafkaUtils.createStream(ssc,zkQuorum,consumerGroup,kakfaTopics)
    val logsStreamCount = logsStream.count()

    //print 10 logs from each DStream batch duration
    logsStream.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
