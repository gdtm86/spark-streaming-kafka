package com.cloudera.sa.transamerica.util

import java.io.{ObjectInputStream, ByteArrayInputStream, ObjectOutputStream, ByteArrayOutputStream}

import kafka.common.TopicAndPartition
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Get, Put, HTable}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.streaming.kafka.OffsetRange
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * Created by gmedasani on 8/24/16.
 */
object KafkaOffsetDAO {

  /*
    Function to insert the offset Ranges for each batch into HBase with batchTime as the rowKey
   */
  def insertIntoHBase(offsets: Array[OffsetRange],zkQuorum:String, tableName:String,batchTime:org.apache.spark.streaming.Time): Unit = {

    //Print the following message to identify the call for HBase in logs
    println("Inserting the offsets for the batch -----------"+batchTime+"---------------")

    //Create a list of offsets from each partition in the topic
    val offsetsNew:ListBuffer[(String,Int,Long,Long)] = ListBuffer[(String,Int,Long,Long)]()
    for (offset <- offsets){
      offsetsNew += ((offset.topic,offset.partition, offset.fromOffset, offset.untilOffset))
      println(offset)
    }
    val offsetsList = offsetsNew.toList

    //Create a byte array from offsetLists
    val byteArrayOutputStream = new ByteArrayOutputStream()
    val objectOutputStream  = new ObjectOutputStream(byteArrayOutputStream)
    objectOutputStream.writeObject(offsetsList)
    objectOutputStream.close()
    val offsetByteArray = byteArrayOutputStream.toByteArray

    //Create a HBase connection and insert the offsetByteArray into the HBase Table
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum",zkQuorum)
    val table = new HTable(hbaseConf,tableName)
    val rowKey = batchTime.milliseconds
    val put = new Put(Bytes.toBytes(rowKey))
    put.addColumn(Bytes.toBytes("tp_offsets"),Bytes.toBytes("offsetRanges"),offsetByteArray)
    table.put(put)
    table.flushCommits()

  }

  /*
    Function to read the offsets from HBase for  batchTime where the last SparkStreaming application stopped or crashed
   */
  def readOffsetsFromHBase(zkQuorum:String,tableName:String,offsetFetchTime:String): mutable.Map[TopicAndPartition,Long] ={

    //Create a HBase connection and get the offsetByteArray from the HBase Table
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum",zkQuorum)
    val table = new HTable(hbaseConf,tableName)
    val rowKey = offsetFetchTime.toLong
    val rowGet = new Get(Bytes.toBytes(rowKey))

    //check if the row exists in HBase and if the row doesn't exist, exit the program
    if(!table.exists(rowGet)){
      println("================================================================================")
      println("offsetFetchTime you entered is invalid. Please enter a valid offsetFetchTime ")
      System.exit(1)
    } //else continue to get the offset from HBase

    val result = table.get(rowGet)

    //Reading the values from Result class object
    val offsetRange: Array[Byte] = result.getValue(Bytes.toBytes("tp_offsets"),Bytes.toBytes("offsetRanges"))

    //Read the bytes back into a list
    val byteArrayInputStream = new ByteArrayInputStream(offsetRange)
    val objectInputStream = new ObjectInputStream(byteArrayInputStream)
    val offsets = objectInputStream.readObject()

    //Get the list from the objects AnyRef
    val offsetsList = offsets.asInstanceOf[List[(String,Int,Long,Long)]]

    //Print the following message to identify the call for HBase in logs
    println("Retrieving the offsets for the first batch using the RowKey-----------"+offsetFetchTime+"---------------")
    println("Received the following offsets for the offsetFetchTime - "+ offsetFetchTime + offsetsList)

    //Convert the lisf of offsets into a Map
    val fromOffsets:scala.collection.mutable.Map[TopicAndPartition,Long] = scala.collection.mutable.Map()
    for (offset <- offsetsList){
      val topicAndPartition = new TopicAndPartition(offset._1,offset._2)
      fromOffsets += (topicAndPartition -> offset._3)
    }
    return fromOffsets
  }

}
