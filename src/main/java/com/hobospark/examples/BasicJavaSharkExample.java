// Basic Shark example in Java

package com.hobospark.examples;

import spark.api.java.JavaRDD;
import spark.api.java.JavaPairRDD;
import spark.api.java.function.PairFunction;

import scala.Tuple2;

import shark.SharkEnv;
import shark.api.Row;
import shark.api.JavaSharkContext;
import shark.api.JavaTableRDD;

public class BasicJavaSharkExample {
  public static void main(String[] args) {
      JavaSharkContext sc = SharkEnv.initWithJavaSharkContext("BasicSharkExample");
      sc.sql("drop table if exists src");
      sc.sql("CREATE TABLE src(key INT, value STRING)");
      sc.sql("LOAD DATA LOCAL INPATH '${env:HIVE_HOME}/examples/files/in1.txt' INTO TABLE src");
      JavaTableRDD rdd = sc.sql2rdd("SELECT src.key, src.value FROM src WHERE src.key < 100");
      rdd.cache();
      System.out.println("Found "+rdd.count()+" num rows");
      JavaPairRDD<Integer, String> normalRDD = rdd.map(
						       new PairFunction<Row, Integer, String>() {
							   @Override
							   public Tuple2<Integer, String> call(Row x) {
							       return new Tuple2<Integer,String>(x.getInt("key"), x.getString("value"));
							   }
						       });
      System.out.println("Collected: "+normalRDD.collect());
   }
}
