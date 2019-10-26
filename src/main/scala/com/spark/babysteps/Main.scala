package com.spark.babysteps

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.log4j.Level;

object Main extends App {
  //Sum of 10
  org.apache.log4j.Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder().master("local[2]").appName("WordCount").getOrCreate();
  import spark.implicits._
  val data = spark.sparkContext.textFile(args(0))
  //Top 10 Words
  val result = data.flatMap(_.split("\\W+")).map(_ -> 1).reduceByKey(_ + _).map(x => x._2 -> x._1).sortByKey(false).map(x => s"${x._2} : ${x._1}").take(10)
  result.foreach(println)
  spark.stop

}