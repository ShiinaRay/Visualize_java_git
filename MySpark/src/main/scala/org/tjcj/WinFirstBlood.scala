package org.tjcj

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 拿一血的胜率
 */
object WinFirstBlood {
  def main(args: Array[String]): Unit = {
    //创建SparkContext对象
    val conf = new SparkConf().setAppName("winFirstBlood").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.textFile("hdfs://hadoop01:9000/javaMkdir/lol.txt").filter(!_.contains("agt"))
      .map(x=>{
        var splits=x.split("\t")
        (Integer.parseInt(splits(16)),Integer.parseInt(splits(24)))
      }).filter(x=>{
      x._2==1
    }).reduceByKey(_+_).map(x=>{
      var d:Double=x._2
      ("win",d)
    }).reduceByKey(_/_).map(x=>{
      (x._1,x._2.formatted("%.2f"))
    }).repartition(1).saveAsTextFile("hdfs://hadoop01:9000/spark/out/out8")
  }
}