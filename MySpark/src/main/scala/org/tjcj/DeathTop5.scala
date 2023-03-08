package org.tjcj

import org.apache.spark.{SparkConf, SparkContext}

object DeathTop5 {
  def main(args: Array[String]): Unit = {
    //1、获取SparkContext对象
    val conf:SparkConf = new SparkConf().setAppName("ban5").setMaster("local[*]")
    val sc:SparkContext = new SparkContext(conf)
    val array = sc.textFile("hdfs://hadoop01:9000/javaMkdir/lol.txt").filter(!_.contains("agt")).
      filter(_.contains("LPL")).map(x=>{
      ( x.split("\t")(9),Integer.parseInt(x.split("\t")(18)))  //（team,kills）
    }).reduceByKey(_+_).sortBy(_._2,false).take(5)
    sc.makeRDD(array).map(x=>{
      var str=""
      str=x._1+","+x._2
      str
    }).repartition(1).saveAsTextFile("hdfs://hadoop01:9000/spark/out/out6")
  }
}