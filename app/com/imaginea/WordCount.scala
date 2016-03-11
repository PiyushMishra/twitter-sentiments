package com.imaginea

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.elasticsearch.spark._

/**
 * Created by piyushm on 9/3/16.
 */
object WordCount {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Twitter")
    conf.set("esNodesKey", args(0))
    conf.set("esPortKey", args(1))

    val sc: SparkContext = new SparkContext(conf)

    val wordCount: RDD[(String, Int)] = sc.esRDD("twitter/tweet").
      flatMap { case (_, tweet) =>
      tweet.get("text").filter(_ != "").get.asInstanceOf[String].split("\\s+")
    }.map(word => (word, 1)).reduceByKey(_ + _)

    wordCount.saveAsTextFile(args(2))
    sc.stop()
  }

}
