package com.practice.wordcount

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCount {
  def main(args : Array[String]){
    val sparkConf = new SparkConf() //creating object of SparkConf
    //now setting app name and master
    sparkConf.setAppName("wordCountProgram").setMaster("local[*]") // local[*] Run Spark locally with as many worker threads as logical cores on your machine.
    
    val sparkContext = new SparkContext(sparkConf);
    val file = sparkContext.textFile("/home/omkar/workspace/sparkWorkspace/WordCount/inputFile") //path of input file
    val words = file.flatMap(_.split(" ")) //splits each token seperated by " "
    val assignOne = words.map((_,1))
    val count = assignOne.reduceByKey(_ + _)
    count.collect().foreach(println)
    
  }
}