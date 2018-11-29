package com.panda.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 处理socket数据
  */
object FileWordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetWorkWordCount")
    /**
      * sparkConf,batch interval
      */
    val ssc = new StreamingContext(conf, Seconds(5))

    val lines = ssc.textFileStream("F:/data/sparkstreaming-test/ss")

    //计算
    val res = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    //输出到控制台
    res.print()

    //启动
    ssc.start()
    //等待处理停止（手动或由于任何错误）
    ssc.awaitTermination()

  }
}
