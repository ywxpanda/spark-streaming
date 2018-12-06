package com.panda.spark.streaming


import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 黑名单过滤
  */
object TransformApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("TransformApp")

    val ssc = new StreamingContext(conf, Seconds(5))

    /**
      * zs => <zs,true>
      * ls => <ls,true>
      */
    val black = List("zs", "ls")
    val blackRDD = ssc.sparkContext.parallelize(black).map((_, true))

    val lines = ssc.socketTextStream("localhost", 6789)
    /**
      * <100000,zs> => (zs,<100000,zs>)
      * <100000,ls> => (ls,<100000,ls>)
      * <100000,ww> => (ww,<100000,ww>)
      */
    val res = lines.map(x => (x.split(",")(1), x)).transform(rdd => {
      rdd.leftOuterJoin(blackRDD).filter(x => x._2._2.getOrElse(None, false) != true).map(x => x._2._1)
    })

    res.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
