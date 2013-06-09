// Basic Shark example in Scala

package com.hobospark.examples

import shark._
import spark.SparkContext._

object BasicSharkExample {
  def main(args: Array[String]) {
    val sc = SharkEnv.initWithSharkContext("BasicSharkExample")
    println("Starting shark requests");
    sc.sql("drop table if exists src");
    sc.sql("CREATE TABLE src(key INT, value STRING)")
    sc.sql("LOAD DATA LOCAL INPATH '${env:HIVE_HOME}/examples/files/in1.txt' INTO TABLE src")
    val rdd = sc.sql2rdd("SELECT src.key, src.value FROM src WHERE src.key < 100")
    rdd.cache()
    println("Found "+rdd.count()+" num rows")
    val normalRDD = rdd.map(x => (x.getInt("src.key"), x.getString("src.value")))
    println("Formatted as "+normalRDD.collect())
  }
}
