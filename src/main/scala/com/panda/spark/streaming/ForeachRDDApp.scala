package com.panda.spark.streaming

import java.sql.Connection

import com.alibaba.druid.pool.DruidDataSource
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * 通过数据库进行计数
  */
object ForeachRDDApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ForeachRDDApp").setMaster("local[2]")

    val ssc = new StreamingContext(conf, Seconds(5))
    val lines = ssc.socketTextStream("localhost", 6789)

    val res = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    // todo 将数据写入mysql


    val t = new Test1()

    println("outer")


    /**
      * 通过分区进行rdd的遍历,每个partition使用一个连接
      */
    res.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        val conn = JdbcConnection.getConn
        partition.foreach(partitionRecord => {
          if (partitionRecord._1 != "") {

            val count = getCount(partitionRecord._1, conn)
            var sql: String = ""
            if (count == 0) {
              sql = "insert into word_count (word,count) values ('" + partitionRecord._1 + "'," + partitionRecord._2 + ")"
            } else {
              sql = "update word_count set count = " + (count + partitionRecord._2) + " where word = '" + partitionRecord._1 + "'"
            }
            conn.createStatement().executeUpdate(sql)
          }
        })
      })
    })

    ssc.start()
    ssc.awaitTermination()

  }

  def getCount(value: String, conn: Connection): Int = {
    val selectSql = "select word,count from word_count where word = '" + value + "'"
    val stmt = conn.createStatement()
    val res = stmt.executeQuery(selectSql)
    var count: Int = 0
    while (res.next()) {
      count = res.getInt("count")
    }
    conn.close()
    count
  }


}
