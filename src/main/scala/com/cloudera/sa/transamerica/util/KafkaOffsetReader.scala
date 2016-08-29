package com.cloudera.sa.transamerica.util

import java.io.{ObjectInputStream, ByteArrayInputStream, ObjectOutputStream, ByteArrayOutputStream}

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Get, Put, HTable}
import org.apache.hadoop.hbase.util.Bytes

/**
 * Created by gmedasani on 8/24/16.
 */
object KafkaOffsetReader {

  def main (args: Array[String]) {

    if(args.length < 3){
      System.err.println("Usage: KafkaOffsetReader <HBase-Zookeeper-Quorum> <HBase-Table-Name> <Rowkey-SparkStreamingBatchMilliSeconds>")
      System.exit(1)
    }

    val zkQuorum = args(0)
    val tableName = args(1)
    val rowKey = args(2).toLong

    //Create a HBase connection and get the offsetByteArray from the HBase Table
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum",zkQuorum)
    val table = new HTable(hbaseConf,tableName)
    val rowGet = new Get(Bytes.toBytes(rowKey))

    val rowExists:Boolean = table.exists(rowGet)

    if(!rowExists){
      println("Rowkey is invalid")
      System.exit(1)
    }

      val result = table.get(rowGet)
      //Reading the values from Result class object
      val offsetRange = result.getValue(Bytes.toBytes("tp_offsets"),Bytes.toBytes("offsetRanges"))

      //Printing the values
      println("Rowkey is:" + rowKey)
      println(offsetRange.toString)

      //Read the bytes back into a list
      val byteArrayInputStream = new ByteArrayInputStream(offsetRange)
      val objectInputStream = new ObjectInputStream(byteArrayInputStream)
      val offsets = objectInputStream.readObject()
      val newOffsets = offsets.asInstanceOf[List[(String,Int,Long,Long)]]
      println(newOffsets)
      println(newOffsets(2))


  }

}
