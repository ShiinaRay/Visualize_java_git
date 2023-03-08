package org.tjcj

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 整个赛年中胜率超过60%
 */
object WinSpark {
  def main(args: Array[String]): Unit = {
    //创建SparkContext对象
    val conf = new SparkConf().setAppName("winFirstBlood").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd= sc.textFile("hdfs://hadoop01:9000/javaMkdir/lol.txt").filter(!_.contains("agt"))
    val rdd1=  rdd.map(x=>{
      var splits =x.split("\t")
      (splits(9),Integer.parseInt(splits(16)))
    }).reduceByKey(_+_)
    val rdd2 = rdd.map(x=>{
      var splits =x.split("\t")
      (splits(9),1)
    }).reduceByKey(_+_)
    rdd1.union(rdd2).reduceByKey((x1,x2)=>{
      var win:Int=0;
      if(x1<x2){
        win = x1*100/x2
      }
      win
    }).filter(_._2>60).map(x=>(x._1,x._2+"%"))
      .repartition(1).saveAsTextFile("hdfs://hadoop01:9000/spark/out/out9")
  }
}