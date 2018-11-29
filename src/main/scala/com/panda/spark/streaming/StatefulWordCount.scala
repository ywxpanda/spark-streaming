package com.panda.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StatefulWordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StatefulWordCount").setMaster("local[2]")

    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("./tmp/")
    val lines = ssc.socketTextStream("localhost", 6789)

    val res = lines.flatMap(_.split(" ")).map((_, 1))

    val state = res.updateStateByKey[Int](updateFunction(_, _))

    state.print()

    ssc.start()
    ssc.awaitTermination()

  }

  /**
    * 当前的值加上原先的值
    *
    * @param currentValues 当前的值
    * @param preValues     之前的值
    * @return
    */
  def updateFunction(currentValues: Seq[Int], preValues: Option[Int]): Option[Int] = {
    val cur = currentValues.sum
    println("cur:" + cur)
    val pre = preValues.getOrElse(0)
    println("pre: " + pre)
    Some(cur + pre)
  }
}
